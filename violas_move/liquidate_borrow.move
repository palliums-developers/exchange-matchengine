script {
use 0x7257c2417e4d1038e1817c8f283ace2e::ViolasToken;

fun main<Token1, Token2>(borrower: address, amount: u64, data: vector<u8>) {
    ViolasToken::liquidate_borrow<Token1, Token2>(borrower, amount, data);
}
}

