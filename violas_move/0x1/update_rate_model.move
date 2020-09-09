script {
use 0x1::ViolasBank2;

fun main<Token>(account: &signer, base_rate: u64, rate_multiplier: u64, rate_jump_multiplier: u64, rate_kink: u64) {
    ViolasBank2::update_rate_model<Token>(account, base_rate, rate_multiplier, rate_jump_multiplier, rate_kink);
}
}
