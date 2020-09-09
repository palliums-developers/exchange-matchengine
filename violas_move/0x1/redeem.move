script {
use 0x1::ViolasBank2;

fun main<Token>(account: &signer, amount: u64, data: vector<u8>) {
    ViolasBank2::redeem<Token>(account, amount, data);
}
}
