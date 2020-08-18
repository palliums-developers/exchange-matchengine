script {
use 0x1::ViolasBank;

fun main(account: &signer, tokenidx: u64, payee: address, amount: u64, data: vector<u8>) {
    ViolasBank::mint(account, tokenidx, payee, amount, data);
}
}
