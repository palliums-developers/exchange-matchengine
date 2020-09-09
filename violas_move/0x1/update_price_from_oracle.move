script {
use 0x1::ViolasBank2;

fun main<Token>(_account: &signer) {
    ViolasBank2::update_price_from_oracle<Token>();
}
}
