script {
use 0x1::ViolasBank2;

fun main<Token>(account: &signer, price: u64) {
    ViolasBank2::update_price<Token>(account, price);
}
}
