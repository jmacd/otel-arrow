// Logging, tracing, etc. :-)

eventheader::define_provider!(
    OTEL,
    "Testing_TestComponent");

#[macro_export]
macro_rules! log {
    ($name:literal) => {
        eventheader::write_event!(OTEL, $name );
    };
    ($name:literal, $($field:tt)*) => {
        eventheader::write_event!(OTEL, $name, $($field)*);
    };
}

#[macro_use]

#[cfg(test)]
mod tests {
    use crate::OTEL;
    use crate::_eh_define_provider_OTEL;
    #[test]
    fn ck() {
	log!("hello");

	// TODO: not as ergo as ...
	const month: &str = "July";
	const days: i32 = 31;
	log!("{month:str} has {days:i32} days", str8("month", month), i32("days", &days));
    }
}
