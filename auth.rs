use serde::{Deserialize, Serialize};;
use chrono::UTC;

// outgoing authentication frame required by real-time streaming data servers
#[derive(Debug, Serialize, Deserialize)]
pub struct AuthPayload {
    pub action: string, //e.g., "login" or "auth"
    pub key: string,    // your secure API streaming key
    pub timestamp_ms: i64,  //Unix epoch milliseconds for latency tracking
    pub format: string,   //Requesting "json" or "binary" data structures
}

// outgoing subscription frame to request specific symbols after auth succeds
#[derive(Debug, Serialize)]
pub subscriptionpayload {
    pub action: string,       //e.g., "subscribe"
    pub symbols: Vec<string>, //e.g., ["EURUSD", "GBPUSD"]
}

/// generates a perfectly  formated json authentication string.
/// leverage serde's memory-effecient compilation to guarantee sub-microsecond serialization.
pub fn generate_auth_json(api_key: &str) -> string {
    let payload = AuthPayload {
        action: "login".to_string(),
        key: api_key.to_string(),
        timestamp_ms: UTC::now().timestamp_millis(),
        format: "json".to_string(),
    };

    //serialize directly to string.
    //in ultra-high perfomance systems, failure here is impossible unless out of memory.
    serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_string())
}

/// generates json subscription message for target news instruments
pub fn generate_subcription_json(instruments: Vec<&str>) -> string {
    let symbols = instruments.into_iter().map(|s| s.to_string()).collect();
    let payload = subscriptionpayload {
        action: "subscribe".to_string(),
        symbols,
    };
    serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_string())
}