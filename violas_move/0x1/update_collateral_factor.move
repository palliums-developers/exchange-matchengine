script {
use 0x1::ViolasBank;

fun main<Token>(account: &signer, factor: u64) {
    ViolasBank::update_collateral_factor<Token>(account, factor);
}
}
