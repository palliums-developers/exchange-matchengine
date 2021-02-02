script {
use 0x7257c2417e4d1038e1817c8f283ace2e::ViolasBank2;

fun main<Token>(account: &signer, amount: u64, data: vector<u8>) {
    if(ViolasBank2::is_published(account) == false) {
	ViolasBank2::publish(account, x"00");
    };
    ViolasBank2::enter_bank<Token>(account, amount);
    ViolasBank2::lock<Token>(account, amount, data);
}
}
