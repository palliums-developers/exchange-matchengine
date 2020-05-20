script {
use 0x7257c2417e4d1038e1817c8f283ace2e::ViolasToken;

fun main<Token>(amount: u64, data: vector<u8>) {
    ViolasToken::redeem<Token>(amount, data);
}
}
