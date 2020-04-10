use 0x7257c2417e4d1038e1817c8f283ace2e::ViolasToken;

fun main(tokenidx: u64, price: u64) {
    ViolasToken::update_price(tokenidx, price);
}
