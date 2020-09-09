script {
use 0x1::ViolasBank2;

fun main<Token>(account: &signer, amount: u64, data: vector<u8>) {
    if(ViolasBank2::is_published(account) == false) {
	ViolasBank2::publish(account, x"00");
    };
    ViolasBank2::enter_bank<Token>(account, amount);
    ViolasBank2::repay_borrow<Token>(account, amount, data);
}
}
