use 0x7257c2417e4d1038e1817c8f283ace2e1041b3396cdbb099eb357bbee024d614::ViolasToken;

fun main(tokenidx: u64, payee: address, amount: u64, data: vector<u8>) {
    ViolasToken::transfer(tokenidx, payee, amount, data)
}
