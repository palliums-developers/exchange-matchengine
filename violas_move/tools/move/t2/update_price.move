script {
use 0x7257c2417e4d1038e1817c8f283ace2e::ViolasBank2;

fun main<Token>(account: &signer, price: u64) {
    ViolasBank2::update_price<Token>(account, price);
}
}
