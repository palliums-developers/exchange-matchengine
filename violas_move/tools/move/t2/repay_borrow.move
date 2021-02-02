script {
use 0x7257c2417e4d1038e1817c8f283ace2e::ViolasBank2;

fun main<Token>(account: &signer, amount: u64, data: vector<u8>) {
    ViolasBank2::repay_borrow<Token>(account, amount, data);
}
}
