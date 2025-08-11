//! Simple example demonstrating the rate limiter functionality.

use otap_df_ratelimit::{Clock, MonoClock, RateLimitConfig, RateLimitProcessor};

fn main() {
    println!("Rate Limiter Example");
    println!("===================");

    // Create a rate limiter configuration
    // Rate limit: 10 messages per second
    // Burst: 5 messages can be processed immediately
    let config = RateLimitConfig {
        limit: 10.0,
        burst: 5,
    };

    println!("Configuration:");
    println!("  Rate limit: {} messages/second", config.limit);
    println!("  Burst size: {} messages", config.burst);

    // Create a rate limit processor for String messages
    let mut processor: RateLimitProcessor<String> = match RateLimitProcessor::new(config) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Failed to create rate limiter: {}", e);
            return;
        }
    };

    println!("\nRate limit processor created successfully!");
    println!(
        "Available tokens: {:.1}",
        processor.limiter.tokens_at(MonoClock.now())
    );

    let _res = match processor.limiter.reserve_n_at(MonoClock.now(), 1) {
        Ok(_) => println!("got it!"),
        Err(_) => panic!("noway"),
    };

    println!("\nThis rate limiter can be used in a pipeline to:");
    println!("- Drop messages when rate limit is exceeded");
    println!("- Allow burst traffic up to the configured burst size");
    println!("- Maintain a steady state rate limit over time");
}
