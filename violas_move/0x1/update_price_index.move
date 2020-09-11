script {
use 0x1::ViolasBank;

fun main(account: &signer, tokenidx: u64, price: u64) {
    ViolasBank::update_price_index(account, 0x1::Vector::empty(), tokenidx, price);
}
}
