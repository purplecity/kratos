package errors

/*
这个文件是 Go 标准库 errors 包的一个镜像封装（Mirror/Alias）。

它没有引入任何新的逻辑或自定义类型，而是将 Go 1.20+ 标准库 errors 包中的核心函数和变量原封不动地重新导出了一遍。

🤔 为什么要这么做？

在 Kratos 或其他大型框架中，这种写法通常出于以下三个目的：

统一导入路径
框架希望开发者在处理错误时，只导入框架自己的 errors 包（例如 github.com/go-kratos/kratos/v2/errors），而不是混用标准库的 errors 和框架的 errors。通过把标准库的功能“搬”进来，开发者只需记住一个导入路径。

版本兼容与安全网
errors.Join 是 Go 1.20 才引入的，errors.ErrUnsupported 也是较新版本才有的。如果框架直接依赖这些符号，在旧版 Go 上编译会报错。通过在自己的包里做一次封装，框架可以在内部做构建标签控制或版本检查，对外则提供一个稳定的 API 表面。

为未来扩展预留钩子
现在这些函数只是简单地透传 stderrors.XXX，但将来框架可能需要在全局层面增加行为（比如自动注入 trace ID、记录错误指标等）。有了这层封装，修改时不需要改动所有业务代码的 import，只需改这一个文件。

📋 逐个解读
导出符号   对应标准库   作用
ErrUnsupported   errors.ErrUnsupported   表示操作不支持的标准哨兵错误

Is(err, target)   errors.Is   判断错误链中是否包含目标错误（支持自定义 Is() 方法）

As(err, target)   errors.As   从错误链中提取特定类型的错误

Unwrap(err)   errors.Unwrap   获取被包装的下一层错误

Join(errs...)   errors.Join   将多个错误合并为一个（Go 1.20+）

⚠️ 注意事项

这不是 Kratos 的业务错误包：Kratos 真正的业务错误定义（带 code/reason/message 的那个）在同目录下的其他文件中。这个文件纯粹是标准库的代理。
完全等价：调用这里的 errors.Is() 和直接调用标准库 errors.Is() 行为 100% 相同，没有任何额外开销。
Go 版本要求：由于使用了 errors.Join 和 errors.ErrUnsupported，使用此包的模块最低需要 Go 1.20。

💡 一句话总结

这是一个标准库 errors 包的透明代理层，目的是让框架使用者只用一个 import 就能同时获得标准错误处理能力和框架自定义错误能力，并为未来的全局增强预留了扩展点。
*/
import (
	stderrors "errors"
)

// ErrUnsupported indicates that a requested operation cannot be performed,
// because it is unsupported. For example, a call to os.Link when using a
// file system that does not support hard links.
//
// Functions and methods should not return this error but should instead
// return an error including appropriate context that satisfies
//
//	errors.Is(err, errors.ErrUnsupported)
//
// either by directly wrapping ErrUnsupported or by implementing an Is method.
//
// Functions and methods should document the cases in which an error
// wrapping this will be returned.
var ErrUnsupported = stderrors.ErrUnsupported

// Is reports whether any error in err's chain matches target.
//
// The chain consists of err itself followed by the sequence of errors obtained by
// repeatedly calling Unwrap.
//
// An error is considered to match a target if it is equal to that target or if
// it implements a method Is(error) bool such that Is(target) returns true.
func Is(err, target error) bool { return stderrors.Is(err, target) }

// As finds the first error in err's chain that matches target, and if so, sets
// target to that error value and returns true.
//
// The chain consists of err itself followed by the sequence of errors obtained by
// repeatedly calling Unwrap.
//
// An error matches target if the error's concrete value is assignable to the value
// pointed to by target, or if the error has a method As(interface{}) bool such that
// As(target) returns true. In the latter case, the As method is responsible for
// setting target.
//
// As will panic if target is not a non-nil pointer to either a type that implements
// error, or to any interface type. As returns false if err is nil.
func As(err error, target any) bool { return stderrors.As(err, target) }

// Unwrap returns the result of calling the Unwrap method on err, if err's
// type contains an Unwrap method returning error.
// Otherwise, Unwrap returns nil.
func Unwrap(err error) error {
	return stderrors.Unwrap(err)
}

// Join returns an error that wraps the given errors. The returned error has a
// method Unwrap() []error that returns the given errors in order.
//
// Join returns nil if errs contains no non-nil error values. If errs contains a
// single non-nil error value, Join returns that error. If errs contains multiple
// non-nil error values, Join returns an error that formats as the concatenation
// of the format of the non-nil error values, separated by "; ". The returned
// error's Unwrap method returns a slice of the non-nil error values.
//
// Join is designed for use in situations where multiple errors may be returned,
// such as when processing a list of items and collecting errors from each item.
// It allows you to combine those errors into a single error value that can be
// returned to the caller.
func Join(errs ...error) error {
	return stderrors.Join(errs...)
}
