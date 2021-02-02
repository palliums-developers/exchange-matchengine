script {
use 0x7257c2417e4d1038e1817c8f283ace2e::ViolasBank2;

fun main<Token>(account: &signer, factor: u64) {
    ViolasBank2::update_collateral_factor<Token>(account, factor);
}
}
