#![no_std]

use core::panic::PanicInfo;

#[used]
#[link_section = "ferrosa:streaming-aggregate:v1"]
static FERROSA_STREAMING_AGGREGATE_ABI: [u8; 57] =
    *b"ferrosa streaming aggregate abi: init/update/finalize f64";

static mut COUNT: f64 = 0.0;
static mut SUM_SQUARES: f64 = 0.0;

#[no_mangle]
pub extern "C" fn init() {
    unsafe {
        COUNT = 0.0;
        SUM_SQUARES = 0.0;
    }
}

#[no_mangle]
pub extern "C" fn update(value: f64) {
    unsafe {
        COUNT += 1.0;
        SUM_SQUARES += value * value;
    }
}

#[no_mangle]
pub extern "C" fn finalize() -> f64 {
    unsafe {
        if COUNT <= 0.0 {
            0.0
        } else {
            sqrt(SUM_SQUARES / COUNT)
        }
    }
}

fn sqrt(value: f64) -> f64 {
    if value <= 0.0 {
        return 0.0;
    }

    let mut estimate = if value >= 1.0 { value } else { 1.0 };
    for _ in 0..16 {
        estimate = 0.5 * (estimate + value / estimate);
    }
    estimate
}

#[panic_handler]
fn panic(_info: &PanicInfo) -> ! {
    loop {}
}
