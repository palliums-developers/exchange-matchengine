script {
use 0x7257c2417e4d1038e1817c8f283ace2e::ViolasBank2;

fun main<Token>(payer: &signer, amount: u64) {
    ViolasBank2::enter_bank<Token>(payer, amount);
}
}
