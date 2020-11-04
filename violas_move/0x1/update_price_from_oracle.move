script {
use 0x1::ViolasBank;

 fun main<Token>(account: &signer) {
    if(ViolasBank::is_published(account) == false) {
	ViolasBank::publish(account, x"00");
    };
    ViolasBank::update_price_from_oracle<Token>(account);
}
}
