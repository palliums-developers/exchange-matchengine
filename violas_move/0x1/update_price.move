script {
use 0x1::ViolasBank;

fun main<Token>(account: &signer, price: u64) {
    ViolasBank::update_price<Token>(account, price);
}
}
