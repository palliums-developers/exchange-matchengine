script {
use 0x1::ViolasBank2;

fun main<Token>(payer: &signer, amount: u64) {
    ViolasBank2::enter_bank<Token>(payer, amount);
}
}
