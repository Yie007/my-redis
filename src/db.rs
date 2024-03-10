use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use tokio::{
    sync::{broadcast, Notify},
    time::{self, Instant},
};

/// `Db`实例的包装类，它的创建是为了执行结束时的清理工作。
///
/// 具体来说，当这个类被 drop 掉的时候，他会通知后台任务关闭。
#[derive(Debug)]
pub(crate) struct DbDropGuard {
    db: Db,
}

/// 共享的数据库操作句柄。
///
/// 当获取到一个`Command`后，它会使用到`Db`来执行命令，
/// 因此每个`Handler`都要拥有一个`Db`实例，也因此我们要使用
/// 类似`Arc`这种方式共享所有权。
/// 所以我们派生Clone trait，`clone()`的时候会调用结构体所有字段的`clone()`。
#[derive(Debug, Clone)]
pub(crate) struct Db {
    // 共享状态的句柄，后台任务会拥有一个`Arc<Shared>`。
    // 我们并不能使用`Arc`获取获取内部数据的可变引用，而我们的数据操作会改变内部数据，
    // 需要使用到可变引用，因此需要使用`Mutex`包裹内部数据。具体见`Shared`和`State`。
    shared: Arc<Shared>,
}

/// 共享状态，也就是真正的数据库部分，包括数据部分和后台任务部分。
///
/// 后台任务其实就是一个负责清理过期`Entry`的任务。
#[derive(Debug)]
struct Shared {
    // 共享的数据状态由`Mutex`包裹，保证数据安全。这是一个`std::sync::Mutex`
    // 而非 tokio 的`Mutex`，这是因为这里锁不需要在线程中传递（拥有锁的时候没有
    // 异步操作），并且关键部分很小。
    state: Mutex<State>,

    // 通知后台任务。
    // 后台任务在下一个要清除的`Entry`的过期时间到来之前都处于休眠状态，
    // 如果休眠的时候数据库要关闭，那么就要通知后台任务也关闭；如果休眠的时候
    // 有新数据加入，加入的新数据的过期时间变成最早的了，那么后台任务就要
    // 修改休眠时间，所以要通知后台任务；如果没有要清理的数据，后台任务就处于
    // 等待通知的状态。
    // 我们使用`Notify`不需要获取它的可变引用，不需要加锁。
    background_task: Notify,
}

/// 数据状态，真正意义上的数据部分。
///
/// 数据库会运行一个后台任务，这个后台任务负责清理过期的`Entry`。
/// 显然我们不能让后台任务一直处于活跃状态，毕竟不是每时每刻都要进行清理工作。
/// 所以我们让它休眠到下一个要被清理的`Entry`的过期时间，也就是说我们要维护一个
/// 按照过期时间从小到大排序的集合，所以我们使用一个`BTreeSet`。
#[derive(Debug)]
struct State {
    // 用一个`HashMap`来存储 key-entry。
    entries: HashMap<String, Entry>,

    // 用一个`BTreeSet`来保存排好序的过期时间及对应的 key。
    // 这能让后台程序方便地查看什么时候该开始清除过期 Entry。
    expirations: BTreeSet<(Instant, String)>,

    // 存储信道名称和对应的广播的发送端。
    // 用于实现发布者/订阅者功能。
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,

    // 在所有`Db`都被 drop 的时候，这个值设置为`true`会告知后台任务退出。
    shutdown: bool,
}

/// `HashMap`中 key-value 中的 value。
#[derive(Debug)]
struct Entry {
    // 数据部分。
    data: Bytes,
    // 过期时间。
    expires_at: Option<Instant>,
}

impl DbDropGuard {
    pub(crate) fn new() -> DbDropGuard {
        DbDropGuard { db: Db::new() }
    }

    pub(crate) fn db(&self) -> Db {
        self.db.clone()
    }
}

impl Drop for DbDropGuard {
    fn drop(&mut self) {
        // 关闭后台任务。
        self.db.shutdown_purge_task();
    }
}

impl Db {
    /// 创建一个新的、空的`Db`实例。创建共享状态并开启异步后台任务来清除过期 Entry。
    pub(crate) fn new() -> Db {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                expirations: BTreeSet::new(),
                pub_sub: HashMap::new(),
                shutdown: false,
            }),
            background_task: Notify::new(),
        });

        // 开启后台异步任务。
        tokio::spawn(purge_expired_tasks(shared.clone()));

        Db { shared }
    }

    /// 根据 key 获取 value。
    ///
    /// # Output
    /// 如果 key 不存在，返回`None`；如果存在，返回`Ok(data)`。
    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        let state = self.shared.state.lock().unwrap();
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    /// 设置 key-entry，这里的 entry 由 value 和一个可选的过期时间组成的。
    ///
    /// 如果 key 已经被设置过了，那么会覆盖原有数据。
    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();
        // 是否应该通知后台任务。
        let mut notify = false;

        let expires_at = expire.map(|duration| {
            // 新插入的`Entry`的过期时间。
            let when = Instant::now() + duration;
            // 如果新插入的`Entry`的过期时间是最早的，
            // 那么就要通知后台任务重新载入。
            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);
            when
        });

        // 插入到`HashMap`中，返回原有数据。
        // 原有数据不存在就为`None`。
        let prev = state.entries.insert(
            key.clone(),
            Entry {
                data: value,
                expires_at,
            },
        );

        // 如果存在原有数据且原有数据有设置过期时间，
        // 将`BTreeSet`中对应的删除。
        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                state.expirations.remove(&(when, key.clone()));
            }
        }

        // 将过期时间插入到`BTreeSet`中。
        if let Some(when) = expires_at {
            state.expirations.insert((when, key));
        }

        // 在通知后台任务前解锁，防止后台任务醒来后还要等待锁。
        drop(state);

        // 按需要通知后台任务。
        if notify {
            self.shared.background_task.notify_one();
        }
    }

    /// 根据订阅的信道的名称，返回`Receiver`。
    ///
    /// 如果订阅的信道不存在，那么会创建这个广播信道。
    pub(crate) fn subscribe(&self, key: String) -> broadcast::Receiver<Bytes> {
        use std::collections::hash_map::Entry;

        let mut state = self.shared.state.lock().unwrap();
        match state.pub_sub.entry(key) {
            // 如果请求的信道已经存在，那么就返回广播接收端
            Entry::Occupied(e) => e.get().subscribe(),
            // 如果不存在，就新建
            Entry::Vacant(e) => {
                // 这个广播信道可以存放`1024`条信息。
                // 一条信息会一直存放，直到所有订阅者都接受到信息后才被删除
                // 当信道容量被占满后，旧的信息会被删除
                let (tx, rx) = broadcast::channel(1024);
                e.insert(tx);
                rx
            }
        }
    }

    /// 向指定信道发送信息，返回信道的订阅者的数量。
    pub(crate) fn publish(&self, key: &str, value: Bytes) -> usize {
        let state = self.shared.state.lock().unwrap();
        state
            .pub_sub
            .get(key)
            // 信道存在，发送信息
            // 如果没有订阅者，那么`send()`会返回错误，返回`0`
            .map(|tx| tx.send(value).unwrap_or(0))
            // 信道不存在，当然也没有订阅者，返回`0`
            .unwrap_or(0)
    }

    /// 通知后台任务关闭。
    ///
    /// 这个函数被`DbDropGuard`的`Drop`实现调用。
    fn shutdown_purge_task(&self) {
        // 通过修改`State::shutdown`来通知后台任务
        // 因此需要获取锁
        let mut state = self.shared.state.lock().unwrap();
        state.shutdown = true;
        // 提前释放锁
        // 不然后台任务被通知后还要等待获取锁
        drop(state);
        self.shared.background_task.notify_one();
    }
}

impl Shared {
    /// 清除过期`Entry`，返回下一个应该被清除的`Entry`的过期时间，
    /// 这样后台任务就能知道可以休眠到什么时候再醒来。
    ///
    /// 如果`BTreeSet`为空或数据库正在关闭，返回`None`。
    fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();
        if state.shutdown {
            // 数据库正在关闭，不存在下一个应该被清除的`Entry`的过期时间。
            return None;
        }

        // 我们下面要用到`&mut state`，但是`lock()`只是返回`MutexGuard`,
        // 由于我们使用了锁，所以同时可变地访问`state.expirations`和`state.entries`
        // 是安全的，但是编译器还不够聪明，所以我们这里获取了真正的`&mut state`。
        let state = &mut *state;

        let now = Instant::now();
        // `BTreeSet`是从小到大排序的
        while let Some((when, key)) = state.expirations.iter().next() {
            if *when > now {
                // 清除任务已经做完了，返回下一个应该被清除的`Entry`的过期时间。
                return Some(*when);
            }
            // 当前时间已经超过了过期时间了，执行清除任务。
            state.entries.remove(key);
            state.expirations.remove(&(*when, key.to_string()));
        }

        // 不存在下一个应该被清除的`Entry`的过期时间，其实就是`BTreeSet`为空。
        None
    }

    /// 如果数据库正在关闭，返回`true`。
    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }
}

impl State {
    /// 返回`BTreeSet`中的第一个(Instant,String)中的`Instant`，
    /// 也就是最小的`Instant`。
    fn next_expiration(&self) -> Option<Instant> {
        // `Instant`实现了Copy trait。
        self.expirations.iter().next().map(|entry| entry.0)
    }
}

/// 异步后台任务，负责清除过期`Entry`。
///
/// 它是周期性执行的，毕竟不能一直处于执行状态，它等待被通知。
/// 当数据更新或收到关闭信号的时候，它会被通知。
async fn purge_expired_tasks(shared: Arc<Shared>) {
    // 被通知后会继续循环，如果发现 shutdown 为真，则退出循环。
    while !shared.is_shutdown() {
        // 清除过期的`Entry`，函数会返回下一个应该被清除的`Entry`的过期时间。
        if let Some(when) = shared.purge_expired_keys() {
            // 我们休眠到上述那个时刻，但是如果该任务在此期间被通知了
            // (数据有更新)，就要重新循环，重新运行`purge_expired_keys()`，
            // 毕竟下一个应该被清除的`Entry`的过期时间对应的`Entry`可能被操作了。
            // 当然也有可能是通知关闭。
            tokio::select! {
                _ = time::sleep_until(when) => {}
                _ = shared.background_task.notified() => {}
            }
        } else {
            // 没有要清除的`Entry`了，等待被通知。
            shared.background_task.notified().await;
        }
    }
}
