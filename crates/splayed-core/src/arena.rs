//! Handle 内部缓存用的极小 arena：`Box` 条目地址稳定，push 后仅共享访问。

use std::cell::RefCell;

/// 追加一个元素并返回其共享引用（生命周期 = 容器借用）。
///
/// 安全性：条目在 `Box` 中（地址稳定）；容器只 push 不 remove/reallocate 条目；
/// push 之后不存在对条目的可变访问。
pub fn push<'a, T>(cell: &'a RefCell<Vec<Box<T>>>, value: T) -> &'a T {
    cell.borrow_mut().push(Box::new(value));
    let vec = cell.borrow();
    let ptr: *const T = &**vec.last().expect("just pushed") as *const T;
    unsafe { &*ptr }
}
