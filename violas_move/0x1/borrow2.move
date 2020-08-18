script {
use 0x1::ViolasBank;

fun main<Token>(account: &signer, amount: u64, data: vector<u8>) {
    ViolasBank::borrow<Token>(account, amount, data);
    ViolasBank::exit_bank<Token>(account, amount);
}
}
