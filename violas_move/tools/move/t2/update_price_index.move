script {
use 0x7257c2417e4d1038e1817c8f283ace2e::ViolasBank2;

fun main(account: &signer, tokenidx: u64, price: u64) {
    ViolasBank2::update_price_index(account, 0x1::Vector::empty(), tokenidx, price);
}
}
