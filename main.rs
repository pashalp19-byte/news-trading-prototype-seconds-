mod auth;

use auth::{generate_auth_json, generate_subcription_json};
use chrono::Utc;
use futures-util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::time::instant;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_asyc, tungstenite::protocol::Message};

// struct matching incoming broker data frames
#[derive(Debug,Deserialize, Clone)]
pub struct MarketTick {
    #[serde(rename = "s")]
    pub symbol: string,
    #[serde(rename = "b")]
    pub bid: f64,
    #[serde(rename = "a")]
    pub ask: f64,
    #[serde(rename = "t")]
    pub server_time_ms: i64,
}

#[tokio::main]
async fn main() {
    //1.setup configurations
    let ws_url = "wss://stream.tradermade.com/feedAdv"; //replace with your target real-time stream endpoint
    let api_key = "YOUR_REAL_TIME_API_KEY"; //replace with your credential key
    let target_symbols = vec!["EURUSD", "GBPUSD"];

    println!(" initializing rust news trading ingestion core...");

    //2.establish secure TLS websocket connection
    let (ws_stream, _) = connect_async(ws_url)
     .await
     .expect("critical: failed to bind to target secure socket");
    let (mut write, mut read) = ws_stream.split();
    println!("  Handshake complete. TCP stream connected.");

    //3. format and send the json authentication message
    let auth_msg = generate_auth_json(api_key);
    write.send(Message::Text(auth_msg)).await.expect("failed to dispatch auth payload");
    println!("   Authentication frame dispatch to server.");

    //4.instantiate lockless bounded channel for logic decoupling
    let(tx, mut rx) = mpsc::channel::<MarketTick>(5000);

    //Task 1: Async ingestion loop (Dedication Worker Thread)
    tokio::spawn(async move {
        let mut authenticated = false;

        while let some(message) = read.next().await {
            match message {
                ok(Message::Text(text)) => {
                    //check if server accepted credentials before parsing data ticks
                    if !authenticated {
                        if text.contains("login_ok") {// common confirmation flag from market data nodes
                            println!("   Authentication confirmed by server!");
                            authenticated = true;

                            //send sub commands immediately
                            let sub_msg = generate_subcription_json(target_symbols.clone());
                            let_ = write.send(Message::Text(sub_msg)).await;
                            println!("  subscription request broadcasted for instruments.");       
                        } else if text.contains("reject") {
                            eprintln!("   Authentication Rejected! Verify streaming credential keys.");
                            break;
                        } 
                        continue;
                    }

                    //perform immediate zero-allocation json extraction
                    if let ok(tick) = serde_json::from_str::<MarketTick>(&text) {
                        if tx.send(tick).await.is_err() {
                            break; // Internal Channel dropped
                        }
                    }
                }
                ok(Message::Close(_)) => {
                    println!("   stream closed by external host.")
                    break;
                }
                _=> {}
            }
        }
    });
    //TASK 2: Logic Evaluation & execution Trigger (Main Processing Thread)
    let mut baseline_bid = 0.0;
     println!("  Processing thread active. monitoring tick matrix...");

    while let some(tick) = rx.rec().await {
        let  processing_timer = Instant::now();
        let spread = tick.ask - tick.bid;

        if baseline_bid > 0.0 {
            let direct_delta = tick.bid - baseline_bid;

            //News Event Tracking Trigger (Example: 4 pip breakout threshold)
            if direct_delta.abs() >= 0.0004 {
                let local_epoch = Utc::now().timestamp_millis();                
                let network_transit_lag = local_epoch - tick.server_time_ms;

                println!("\n=== [NEWS BREAKOUT VOLATILITY MATCH] ===");
                println!("instrument: {} | move: {:.5} | Active spread: {:.5}", ticksymbol, direct_delta, spread);
                println!("Transit Delay: {}ms", network_transit_lag);

                //safe simulation verfication  guards  for your $10 account
                if spread > 0.00015 {
                    println!("   execution aborted: spread too wide ({:.5}). A $10 account will instantly margin call.", spread);
                } else if network_transit_lag > 120 {
                    println!("  execution aborted: high network jitter ({}ms). order entry would face massive slippage.", network_transit_lag);
                } else {
                    let internal latency = processing_timer.elasped().as_micros();
                    println!("  execution signal sent vai local loop bridge in {}μs!", internal_latency);

                    //this is where you call your local execution 
                    // execute_exness_order_via_bridge("buy", tick.symbol, 0.01);
                }
            }
        }
        baseline_bid = tick.bid;
    }
}
