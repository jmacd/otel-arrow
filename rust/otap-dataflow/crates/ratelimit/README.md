# Rate Limit Processor

A simple-as-possible rate limiting processor for the OTAP dataflow engine.

## Features

- **Token Bucket Algorithm**: Uses a well-established token bucket algorithm for rate limiting
- **Simple Configuration**: Just two parameters: `limit` (f64) and `burst` (usize)
- **Drop Policy**: Messages exceeding the rate limit are silently dropped
- **Single-threaded**: Designed for use within single-threaded async environments

## Configuration

The rate limiter has two configurable parameters:

- **`limit`** (f64): Rate limit in messages per second
- **`burst`** (usize): Maximum number of messages that can be processed in a burst

## Usage

### Basic Configuration

```rust
use otap_df_ratelimit::{RateLimitConfig, RateLimitProcessor};

// Create configuration
let config = RateLimitConfig {
    limit: 100.0,  // 100 messages per second
    burst: 10,     // Allow bursts up to 10 messages
};

// Create processor
let processor: RateLimitProcessor<String> = RateLimitProcessor::new(config)?;
```

### Default Configuration

```rust
let config = RateLimitConfig::default();
// Default: 1000 messages/second with burst of 100
```

## Examples

Run the example to see the rate limiter in action:

```bash
cargo run --example simple_rate_limit
```

## How It Works

1. **Token Bucket**: The rate limiter maintains a bucket of tokens that refills at the configured rate
2. **Message Processing**: Each message consumes one token
3. **Burst Handling**: Up to `burst` tokens can be available at once, allowing for traffic spikes
4. **Rate Limiting**: When no tokens are available, messages are dropped
5. **Token Refill**: Tokens are continuously added to the bucket at the `limit` rate

## Design Principles

- **Simple**: Only two configuration parameters
- **Fast**: Minimal overhead per message
- **Predictable**: Clear behavior when rate limits are exceeded
- **Non-blocking**: Never blocks the pipeline, only drops messages

## Integration

This processor is designed to be integrated into OTAP dataflow pipelines where rate limiting is needed to protect downstream systems or comply with rate limits imposed by external services.
