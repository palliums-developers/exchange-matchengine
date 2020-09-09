script {
use 0x1::ViolasBank2;

fun main(account: &signer, tokenidx: u64, payee: address, amount: u64, data: vector<u8>) {
    ViolasBank2::mint(account, tokenidx, payee, amount, data);
}
}
