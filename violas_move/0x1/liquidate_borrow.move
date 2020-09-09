script {
use 0x1::ViolasBank2;

fun main<Token1, Token2>(account: &signer, borrower: address, amount: u64, data: vector<u8>) {
    ViolasBank2::liquidate_borrow<Token1, Token2>(account, borrower, amount, data);
}
}

