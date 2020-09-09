script {
use 0x1::ViolasBank2;

fun main<Token>(account: &signer, amount: u64, data: vector<u8>) {
    if(ViolasBank2::is_published(account) == false) {
	ViolasBank2::publish(account, x"00");
    };
    ViolasBank2::borrow<Token>(account, amount, data);
    ViolasBank2::exit_bank<Token>(account, amount);
}
}
