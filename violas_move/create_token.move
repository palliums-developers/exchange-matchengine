use 0x7257c2417e4d1038e1817c8f283ace2e1041b3396cdbb099eb357bbee024d614::ViolasToken;

fun main(owner: address, tokendata: vector<u8>) {
    ViolasToken::create_token(owner, tokendata)
}
