script {
use 0x1::ViolasBank2;

fun main<Token>(account: &signer, factor: u64) {
    ViolasBank2::update_collateral_factor<Token>(account, factor);
}
}
