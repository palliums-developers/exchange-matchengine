script {
use 0x1::ViolasBank;

fun main<Token>(account: &signer, amount: u64) {
    ViolasBank::exit_bank<Token>(account, amount);
}
}
