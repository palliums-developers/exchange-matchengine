script {
use 0x1::ViolasBank;

fun main<Token>(payer: &signer, amount: u64) {
    ViolasBank::enter_bank<Token>(payer, amount);
}
}
