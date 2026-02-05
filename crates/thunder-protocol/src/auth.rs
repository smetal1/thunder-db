//! Authentication helpers for wire protocols
//!
//! Provides password hashing and verification for:
//! - MD5 authentication (PostgreSQL)
//! - SCRAM-SHA-256 authentication (PostgreSQL)
//! - Native password (MySQL)
//! - Caching SHA2 (MySQL)
//!
//! All password comparisons use constant-time operations to prevent timing attacks.

use md5::{Md5, Digest};
use sha2::Sha256;
use hmac::{Hmac, Mac};
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

/// Compute MD5 hash for PostgreSQL authentication
/// Formula: "md5" + md5(md5(password + user) + salt)
pub fn compute_md5_hash(password: &str, user: &str, salt: &[u8; 4]) -> String {
    // First hash: md5(password + user)
    let mut hasher = Md5::new();
    hasher.update(password.as_bytes());
    hasher.update(user.as_bytes());
    let first_hash = hasher.finalize();
    let first_hex = hex::encode(first_hash);

    // Second hash: md5(first_hex + salt)
    let mut hasher = Md5::new();
    hasher.update(first_hex.as_bytes());
    hasher.update(salt);
    let second_hash = hasher.finalize();

    format!("md5{}", hex::encode(second_hash))
}

/// Verify a plain text password using constant-time comparison.
/// This prevents timing attacks that could leak password information.
pub fn verify_password(provided: &str, expected: &str) -> bool {
    // Use constant-time comparison to prevent timing attacks
    provided.as_bytes().ct_eq(expected.as_bytes()).into()
}

/// Verify an MD5 hash using constant-time comparison.
/// This is used for PostgreSQL MD5 authentication.
pub fn verify_md5_hash(provided: &str, expected: &str) -> bool {
    provided.as_bytes().ct_eq(expected.as_bytes()).into()
}

/// Hash a password with SHA-256
pub fn sha256_hash(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// HMAC-SHA-256
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC key error");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

/// XOR two byte arrays
pub fn xor_bytes(a: &[u8], b: &[u8]) -> Vec<u8> {
    a.iter().zip(b.iter()).map(|(x, y)| x ^ y).collect()
}

/// Generate a random salt
pub fn generate_salt(length: usize) -> Vec<u8> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..length).map(|_| rng.gen()).collect()
}

/// PBKDF2-HMAC-SHA256 key derivation
pub fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut result = [0u8; 32];

    // PBKDF2 with HMAC-SHA256
    let mut u = hmac_sha256(password, &[salt, &[0, 0, 0, 1]].concat());
    result.copy_from_slice(&u);

    for _ in 1..iterations {
        u = hmac_sha256(password, &u);
        for (i, byte) in u.iter().enumerate() {
            result[i] ^= byte;
        }
    }

    result
}

// ============================================================================
// SCRAM-SHA-256 Authentication
// ============================================================================

/// SCRAM-SHA-256 client state
pub struct ScramClient {
    username: String,
    password: String,
    client_nonce: String,
    server_nonce: Option<String>,
    salt: Option<Vec<u8>>,
    iterations: Option<u32>,
    auth_message: String,
}

impl ScramClient {
    pub fn new(username: &str, password: &str) -> Self {
        let client_nonce = generate_nonce();
        Self {
            username: username.to_string(),
            password: password.to_string(),
            client_nonce,
            server_nonce: None,
            salt: None,
            iterations: None,
            auth_message: String::new(),
        }
    }

    /// Generate client-first message
    pub fn client_first_message(&mut self) -> String {
        let bare = format!("n={},r={}", self.username, self.client_nonce);
        self.auth_message = bare.clone();
        format!("n,,{}", bare)
    }

    /// Process server-first message and generate client-final message
    pub fn client_final_message(&mut self, server_first: &str) -> Result<String, String> {
        // Parse server-first message
        let mut server_nonce = None;
        let mut salt = None;
        let mut iterations = None;

        for part in server_first.split(',') {
            if let Some(value) = part.strip_prefix("r=") {
                server_nonce = Some(value.to_string());
            } else if let Some(value) = part.strip_prefix("s=") {
                salt = Some(base64_decode(value)?);
            } else if let Some(value) = part.strip_prefix("i=") {
                iterations = Some(value.parse().map_err(|_| "Invalid iterations")?);
            }
        }

        let server_nonce = server_nonce.ok_or("Missing server nonce")?;
        let salt = salt.ok_or("Missing salt")?;
        let iterations = iterations.ok_or("Missing iterations")?;

        // Verify server nonce starts with client nonce
        if !server_nonce.starts_with(&self.client_nonce) {
            return Err("Server nonce doesn't start with client nonce".into());
        }

        self.server_nonce = Some(server_nonce.clone());
        self.salt = Some(salt.clone());
        self.iterations = Some(iterations);

        // Build auth message
        let client_final_without_proof = format!("c=biws,r={}", server_nonce);
        self.auth_message = format!("{},{},{}", self.auth_message, server_first, client_final_without_proof);

        // Calculate proof
        let salted_password = pbkdf2_sha256(self.password.as_bytes(), &salt, iterations);
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key = sha256_hash(&client_key);
        let client_signature = hmac_sha256(&stored_key, self.auth_message.as_bytes());
        let client_proof = xor_bytes(&client_key, &client_signature);

        Ok(format!("{},p={}", client_final_without_proof, base64_encode(&client_proof)))
    }

    /// Verify server-final message using constant-time comparison.
    pub fn verify_server_final(&self, server_final: &str) -> Result<(), String> {
        let server_signature = server_final.strip_prefix("v=")
            .ok_or("Invalid server-final message")?;

        let salt = self.salt.as_ref().ok_or("No salt")?;
        let iterations = self.iterations.ok_or("No iterations")?;

        let salted_password = pbkdf2_sha256(self.password.as_bytes(), salt, iterations);
        let server_key = hmac_sha256(&salted_password, b"Server Key");
        let expected_signature = hmac_sha256(&server_key, self.auth_message.as_bytes());

        let provided_signature = base64_decode(server_signature)?;

        // Use constant-time comparison to prevent timing attacks
        if bool::from(provided_signature.ct_eq(expected_signature.as_slice())) {
            Ok(())
        } else {
            Err("Server signature mismatch".into())
        }
    }
}

/// SCRAM-SHA-256 server state
pub struct ScramServer {
    stored_key: [u8; 32],
    server_key: [u8; 32],
    salt: Vec<u8>,
    iterations: u32,
    server_nonce: String,
    auth_message: String,
}

impl ScramServer {
    /// Create from a stored password hash
    pub fn new(password: &str, iterations: u32) -> Self {
        let salt = generate_salt(16);
        let salted_password = pbkdf2_sha256(password.as_bytes(), &salt, iterations);
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key = sha256_hash(&client_key);
        let server_key = hmac_sha256(&salted_password, b"Server Key");

        Self {
            stored_key,
            server_key,
            salt,
            iterations,
            server_nonce: generate_nonce(),
            auth_message: String::new(),
        }
    }

    /// Process client-first message and generate server-first message
    pub fn server_first_message(&mut self, client_first: &str) -> Result<String, String> {
        // Parse client-first message (strip "n,,")
        let bare = client_first.strip_prefix("n,,").ok_or("Invalid client-first")?;

        let mut client_nonce = None;
        for part in bare.split(',') {
            if let Some(value) = part.strip_prefix("r=") {
                client_nonce = Some(value.to_string());
            }
        }

        let client_nonce = client_nonce.ok_or("Missing client nonce")?;
        let combined_nonce = format!("{}{}", client_nonce, self.server_nonce);

        self.auth_message = bare.to_string();

        let response = format!(
            "r={},s={},i={}",
            combined_nonce,
            base64_encode(&self.salt),
            self.iterations
        );

        self.auth_message = format!("{},{}", self.auth_message, response);
        self.server_nonce = combined_nonce;

        Ok(response)
    }

    /// Process client-final message and generate server-final message.
    /// Uses constant-time comparison for proof verification.
    pub fn server_final_message(&mut self, client_final: &str) -> Result<String, String> {
        // Parse client-final message
        let mut proof = None;
        let mut without_proof = Vec::new();

        for part in client_final.split(',') {
            if let Some(value) = part.strip_prefix("p=") {
                proof = Some(base64_decode(value)?);
            } else {
                without_proof.push(part);
            }
        }

        let proof = proof.ok_or("Missing proof")?;
        let client_final_without_proof = without_proof.join(",");

        self.auth_message = format!("{},{}", self.auth_message, client_final_without_proof);

        // Verify proof using constant-time comparison
        let client_signature = hmac_sha256(&self.stored_key, self.auth_message.as_bytes());
        let client_key = xor_bytes(&proof, &client_signature);
        let computed_stored_key = sha256_hash(&client_key);

        // Use constant-time comparison to prevent timing attacks
        if !bool::from(computed_stored_key.ct_eq(&self.stored_key)) {
            return Err("Authentication failed".into());
        }

        // Generate server signature
        let server_signature = hmac_sha256(&self.server_key, self.auth_message.as_bytes());
        Ok(format!("v={}", base64_encode(&server_signature)))
    }
}

// ============================================================================
// MySQL Authentication
// ============================================================================

/// MySQL native password authentication
/// Hash: SHA1(password) XOR SHA1(scramble + SHA1(SHA1(password)))
pub fn mysql_native_password(password: &str, scramble: &[u8]) -> Vec<u8> {
    use sha1::{Sha1, Digest as Sha1Digest};

    // SHA1(password)
    let mut hasher = Sha1::new();
    hasher.update(password.as_bytes());
    let hash1 = hasher.finalize();

    // SHA1(SHA1(password))
    let mut hasher = Sha1::new();
    hasher.update(&hash1);
    let hash2 = hasher.finalize();

    // SHA1(scramble + SHA1(SHA1(password)))
    let mut hasher = Sha1::new();
    hasher.update(scramble);
    hasher.update(&hash2);
    let hash3 = hasher.finalize();

    // XOR hash1 with hash3
    hash1.iter().zip(hash3.iter()).map(|(a, b)| a ^ b).collect()
}

/// MySQL caching_sha2_password authentication
pub fn mysql_caching_sha2(password: &str, scramble: &[u8]) -> Vec<u8> {
    // SHA256(password)
    let hash1 = sha256_hash(password.as_bytes());

    // SHA256(SHA256(password))
    let hash2 = sha256_hash(&hash1);

    // SHA256(SHA256(SHA256(password)) + scramble)
    let mut combined = hash2.to_vec();
    combined.extend_from_slice(scramble);
    let hash3 = sha256_hash(&combined);

    // XOR hash1 with hash3
    xor_bytes(&hash1, &hash3)
}

// ============================================================================
// Utility Functions
// ============================================================================

fn generate_nonce() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let bytes: Vec<u8> = (0..18).map(|_| rng.gen()).collect();
    base64_encode(&bytes)
}

fn base64_encode(data: &[u8]) -> String {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    STANDARD.encode(data)
}

fn base64_decode(data: &str) -> Result<Vec<u8>, String> {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    STANDARD.decode(data).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_md5_hash() {
        let hash = compute_md5_hash("password", "user", &[0x01, 0x02, 0x03, 0x04]);
        assert!(hash.starts_with("md5"));
        assert_eq!(hash.len(), 35); // "md5" + 32 hex chars
    }

    #[test]
    fn test_sha256() {
        let hash = sha256_hash(b"test");
        assert_eq!(hash.len(), 32);
    }

    #[test]
    fn test_xor_bytes() {
        let a = vec![0xFF, 0x00, 0xAA];
        let b = vec![0x0F, 0xF0, 0x55];
        let result = xor_bytes(&a, &b);
        assert_eq!(result, vec![0xF0, 0xF0, 0xFF]);
    }

    #[test]
    fn test_generate_salt() {
        let salt = generate_salt(16);
        assert_eq!(salt.len(), 16);
    }

    #[test]
    fn test_verify_password_constant_time() {
        // Test correct password
        assert!(verify_password("correct_password", "correct_password"));

        // Test wrong password
        assert!(!verify_password("wrong_password", "correct_password"));

        // Test different lengths (should still work correctly)
        assert!(!verify_password("short", "longer_password"));
        assert!(!verify_password("longer_password", "short"));

        // Test empty strings
        assert!(verify_password("", ""));
        assert!(!verify_password("", "nonempty"));
    }

    #[test]
    fn test_verify_md5_hash_constant_time() {
        let hash1 = compute_md5_hash("password", "user", &[0x01, 0x02, 0x03, 0x04]);
        let hash2 = compute_md5_hash("password", "user", &[0x01, 0x02, 0x03, 0x04]);
        let hash3 = compute_md5_hash("different", "user", &[0x01, 0x02, 0x03, 0x04]);

        assert!(verify_md5_hash(&hash1, &hash2));
        assert!(!verify_md5_hash(&hash1, &hash3));
    }
}
