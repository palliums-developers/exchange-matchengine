script {
use 0x7257c2417e4d1038e1817c8f283ace2e::ViolasBank2;

fun main<Token>(account: &signer) {
    ViolasBank2::update_price_from_oracle<Token>(account);
}
}
