use std::str::FromStr;

use snow::{params::{CipherChoice, HandshakeChoice, HandshakeModifierList, HandshakePattern, NoiseParams}, Builder};


pub const NOISE_PARAMS: &'static str = "Noise_XX_25519_AESGCM_SHA256";

pub fn initialize_noise() {

    let params = NoiseParams{ 
        name: NOISE_PARAMS.to_string(),
        base: snow::params::BaseChoice::Noise,
        handshake: HandshakeChoice {
            pattern: HandshakePattern::XX,
            modifiers: HandshakeModifierList::from_str("").unwrap(),
        },
        dh: snow::params::DHChoice::Curve25519,
        cipher: CipherChoice::AESGCM,
        hash: snow::params::HashChoice::SHA256,
    };

    let builder = Builder::new(params);
    let static_key = builder.generate_keypair().unwrap().private;
    let mut handshakestate = builder
        .local_private_key(&static_key)
        .build_responder()
        .unwrap();




}