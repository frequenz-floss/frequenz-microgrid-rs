// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! This module defines various physical quantities and their operations.

/// A trait for physical quantities that supports basic arithmetic operations.
pub trait Quantity:
    std::ops::Add<Output = Self>
    + std::ops::Sub<Output = Self>
    + std::ops::Mul<Percentage, Output = Self>
    + std::ops::Mul<f32, Output = Self>
    + std::ops::Div<f32, Output = Self>
    + std::ops::Div<Self, Output = f32>
    + std::cmp::PartialOrd
    + std::fmt::Display
    + Copy
    + Clone
    + std::fmt::Debug
    + Default
    + Sized
    + Send
    + Sync
{
    fn zero() -> Self {
        Self::default()
    }

    fn abs(self) -> Self;
    fn floor(self) -> Self;
    fn ceil(self) -> Self;
    fn round(self) -> Self;
    fn trunc(self) -> Self;
    fn fract(self) -> Self;
    fn is_nan(self) -> bool;
    fn is_infinite(self) -> bool;
    fn min(self, other: Self) -> Self;
    fn max(self, other: Self) -> Self;
}

impl std::ops::Mul<Percentage> for f32 {
    type Output = f32;

    fn mul(self, other: Percentage) -> Self::Output {
        self * other.as_fraction()
    }
}

impl Quantity for f32 {
    fn abs(self) -> Self {
        self.abs()
    }

    fn floor(self) -> Self {
        self.floor()
    }

    fn ceil(self) -> Self {
        self.ceil()
    }

    fn round(self) -> Self {
        self.round()
    }

    fn trunc(self) -> Self {
        self.trunc()
    }

    fn fract(self) -> Self {
        self.fract()
    }

    fn is_nan(self) -> bool {
        self.is_nan()
    }

    fn is_infinite(self) -> bool {
        self.is_infinite()
    }

    fn min(self, other: Self) -> Self {
        self.min(other)
    }

    fn max(self, other: Self) -> Self {
        self.max(other)
    }
}

/// Formats an f32 with a given precision and removes trailing zeros
fn format_float(value: f32, precision: usize) -> String {
    let mut s = format!("{:.1$}", value, precision);
    if s.contains('.') {
        s = s.trim_end_matches('0').to_string();
    }
    if s.ends_with('.') {
        s.pop();
    }
    s
}

macro_rules! qty_format {
    (@impl $self:ident, $f:ident, $prec:ident,
        ($ctor:ident, $getter:ident, $unit:literal, $exp:literal),
    ) => {
        write!($f, "{} {}", format_float( $self.$getter(), $prec), $unit)
    };

    (@impl $self:ident, $f:ident, $prec:ident,
        ($ctor1:ident, $getter1:ident, $unit1:literal, $exp1:literal),
        ($ctor2:ident, $getter2:ident, $unit2:literal, $exp2:literal), $($rest:tt)*
    ) => {{
        const {assert!($exp1 < $exp2, "Units must be in increasing order of magnitude.")};

        if $exp1 <= $self.value.abs() && $self.value.abs() < $exp2 {
            write!($f, "{} {}", format_float( $self.$getter1(), $prec), $unit1)
        } else {
            qty_format!(@impl $self, $f, $prec, ($ctor2, $getter2, $unit2, $exp2), $($rest)*)
        }}
    };

    (@impl $self:ident, $f:ident, $prec:ident,
        ($ctor1:ident, $getter1:ident, $unit1:literal, $exp1:literal),
        ($ctor2:ident, $getter2:ident, None, $exp2:literal),
    ) => {
        write!($f, "{} {}", format_float( $self.$getter1(), $prec), $unit1)
    };

    (@start $self:ident, $f:ident, $prec:ident,
        ($ctor:ident, $getter:ident, $unit:literal, $exp:literal), $($rest:tt)*
    ) => {
        if $self.value.abs() <= $exp {
            write!($f, "{} {}", format_float( $self.$getter(), $prec), $unit)
        } else {
            qty_format!(@impl $self, $f, $prec, ($ctor, $getter, $unit, $exp), $($rest)*)
        }
    };

    ($typename:ident => {$($rest:tt)*}) => {
        use super::format_float;
        impl std::fmt::Display for $typename {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                let prec = if let Some(prec) = f.precision() {
                    prec
                } else {
                    3
                };
                qty_format!(@start self, f, prec, $($rest)*)
            }

        }
    };
}

macro_rules! qty_ctor {
    (@impl ($ctor:ident, $getter:ident, $unit:tt, $exp:literal) $(,)?) => {
        pub const fn $ctor(value: f32) -> Self {
            Self { value: value * $exp }
        }
        pub const fn $getter(&self) -> f32 {
            self.value / $exp
        }
    };
    (@impl ($ctor:ident, $getter:ident, $unit:tt, $exp:literal), $($rest:tt)*) => {
        qty_ctor!(@impl ($ctor, $getter, $unit, $exp));
        qty_ctor!(@impl $($rest)*);
    };
    (@impl_arith_ops $typename:ident) => {
        impl std::ops::Add for $typename {
            type Output = Self;

            fn add(self, rhs: Self) -> Self::Output {
                Self {
                    value: self.value + rhs.value,
                }
            }
        }

        impl std::ops::Sub for $typename {
            type Output = Self;

            fn sub(self, rhs: Self) -> Self::Output {
                Self {
                    value: self.value - rhs.value,
                }
            }
        }

        impl std::ops::Mul<super::Percentage> for $typename {
            type Output = Self;

            fn mul(self, other: super::Percentage) -> Self::Output {
                Self {
                    value: self.value * other.as_fraction(),
                }
            }
        }

        impl std::ops::Mul<f32> for $typename {
            type Output = Self;

            fn mul(self, other: f32) -> Self::Output {
                Self {
                    value: self.value * other,
                }
            }
        }

        impl std::ops::Div<f32> for $typename {
            type Output = Self;

            fn div(self, other: f32) -> Self::Output {
                Self {
                    value: self.value / other,
                }
            }
        }

        impl std::ops::Div<$typename> for $typename {
            type Output = f32;

            fn div(self, other: Self) -> Self::Output {
                self.value / other.value
            }
        }

        impl std::cmp::PartialOrd for $typename {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                self.value.partial_cmp(&other.value)
            }
        }

    };
    (#[$meta:meta] $typename:ident => {$($rest:tt)*}) => {
        #[$meta]
        #[derive(Copy, Clone, Debug, Default, PartialEq)]
        pub struct $typename {
            value: f32,
        }

        impl $typename {
            qty_ctor!(@impl $($rest)*);

            pub const fn abs(self) -> Self {
                Self {
                    value: self.value.abs(),
                }
            }

            pub const fn floor(self) -> Self {
                Self {
                    value: self.value.floor(),
                }
            }

            pub const fn ceil(self) -> Self {
                Self {
                    value: self.value.ceil(),
                }
            }

            pub const fn round(self) -> Self {
                Self {
                    value: self.value.round(),
                }
            }

            pub const fn trunc(self) -> Self {
                Self {
                    value: self.value.trunc(),
                }
            }

            pub const fn fract(self) -> Self {
                Self {
                    value: self.value.fract(),
                }
            }

            pub const fn is_nan(self) -> bool {
                self.value.is_nan()
            }

            pub const fn is_infinite(self) -> bool {
                self.value.is_infinite()
            }

            pub const fn min(self, other: Self) -> Self {
                Self {
                    value: self.value.min(other.value),
                }
            }

            pub const fn max(self, other: Self) -> Self {
                Self {
                    value: self.value.max(other.value),
                }
            }
        }

        qty_ctor!{@impl_arith_ops $typename}
        qty_format!{$typename => {$($rest)*}}

        impl super::Quantity for $typename {
            fn abs(self) -> Self {
                self.abs()
            }

            fn floor(self) -> Self {
                self.floor()
            }

            fn ceil(self) -> Self {
                self.ceil()
            }

            fn round(self) -> Self {
                self.round()
            }

            fn trunc(self) -> Self {
                self.trunc()
            }

            fn fract(self) -> Self {
                self.fract()
            }

            fn is_nan(self) -> bool {
                self.is_nan()
            }

            fn is_infinite(self) -> bool {
                self.is_infinite()
            }

            fn min(self, other: Self) -> Self {
                self.min(other)
            }

            fn max(self, other: Self) -> Self {
                self.max(other)
            }
        }
    };
}

mod current;
mod energy;
mod frequency;
mod percentage;
mod power;
mod reactive_power;
mod voltage;

pub use current::Current;
pub use energy::Energy;
pub use frequency::Frequency;
pub use percentage::Percentage;
pub use power::Power;
pub use reactive_power::ReactivePower;
pub use voltage::Voltage;

#[cfg(test)]
mod test_utils {
    /// Asserts that two f32 values are approximately equal within a small epsilon.
    #[track_caller]
    pub(crate) fn assert_f32_eq(a: f32, b: f32) {
        let epsilon: f32 = 10.0_f32.powf(a.log10().min(b.log10())) * 1e-6;
        if (a - b).abs() > epsilon {
            panic!(
                "assertion failed: `(left ~= right)` (epsilon: {})\n left: `{}`,\n right: `{}`",
                epsilon, a, b
            );
        }
    }
}
