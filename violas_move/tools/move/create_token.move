script {
use 0x1::ViolasBank;

fun main(account: &signer, currency_code: vector<u8>, owner: address, price_oracle: address, collateral_factor: u64, base_rate: u64, rate_multiplier: u64, rate_jump_multiplier: u64, rate_kink: u64, tokendata: vector<u8>) {
    ViolasBank::create_token(account, currency_code, owner, price_oracle, collateral_factor, base_rate, rate_multiplier, rate_jump_multiplier, rate_kink, tokendata);
}
}
