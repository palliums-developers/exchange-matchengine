script {
use 0x1::ViolasBank;

fun main<Token>(account: &signer) {
    ViolasBank::update_price_from_oracle<Token>(account);
}
}
