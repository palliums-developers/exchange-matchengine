script {
use 0x1::ViolasBank2;

fun main<Token>(account: &signer, amount: u64) {
    ViolasBank2::exit_bank<Token>(account, amount);
}
}
