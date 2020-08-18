script {
use 0x1::ViolasBank;

fun main<Token>(account: &signer, amount: u64, data: vector<u8>) {
    ViolasBank::lock<Token>(account, amount, data);
}
}
