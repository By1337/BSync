package dev.by1337.sync.common.security;

import java.security.*;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

public class Ed25519 {

    private static final String ALGORITHM = "Ed25519";

    public static KeyPair generateKeyPair() throws NoSuchAlgorithmException {
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(ALGORITHM);
        return keyPairGenerator.generateKeyPair();
    }

    public static byte[] sign(PrivateKey privateKey, byte[] data)
            throws NoSuchAlgorithmException, InvalidKeyException, SignatureException {
        Signature signature = Signature.getInstance(ALGORITHM);
        signature.initSign(privateKey);
        signature.update(data);
        return signature.sign();
    }

    public static boolean verify(PublicKey publicKey, byte[] data, byte[] signature)
            throws NoSuchAlgorithmException, InvalidKeyException, SignatureException {
        Signature sig = Signature.getInstance(ALGORITHM);
        sig.initVerify(publicKey);
        sig.update(data);
        return sig.verify(signature);
    }

    public static String privateKeyToBase64(PrivateKey privateKey) {
        return Base64.getEncoder().encodeToString(privateKey.getEncoded());
    }

    public static String publicKeyToBase64(PublicKey publicKey) {
        return Base64.getEncoder().encodeToString(publicKey.getEncoded());
    }

    public static String signatureToBase64(byte[] signature) {
        return Base64.getEncoder().encodeToString(signature);
    }

    public static PrivateKey privateKeyFromBase64(String base64Key)
            throws Exception {
        byte[] keyBytes = Base64.getDecoder().decode(base64Key);
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
        KeyFactory keyFactory = KeyFactory.getInstance(ALGORITHM);
        return keyFactory.generatePrivate(spec);
    }

    public static PublicKey publicKeyFromBase64(String base64Key)
            throws Exception {
        byte[] keyBytes = Base64.getDecoder().decode(base64Key);
        X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);
        KeyFactory keyFactory = KeyFactory.getInstance(ALGORITHM);
        return keyFactory.generatePublic(spec);
    }

    public static byte[] signatureFromBase64(String base64Signature) {
        return Base64.getDecoder().decode(base64Signature);
    }

    public static String signToBase64(PrivateKey privateKey, byte[] data)
            throws NoSuchAlgorithmException, InvalidKeyException, SignatureException {
        byte[] signature = sign(privateKey, data);
        return signatureToBase64(signature);
    }

    public static boolean verifyFromBase64(PublicKey publicKey, byte[] data, String base64Signature)
            throws NoSuchAlgorithmException, InvalidKeyException, SignatureException {
        byte[] signature = signatureFromBase64(base64Signature);
        return verify(publicKey, data, signature);
    }

    public static KeyPairStore saveKeyPairToBase64(KeyPair keyPair) {
        KeyPairStore store = new KeyPairStore();
        store.setPrivateKey(privateKeyToBase64(keyPair.getPrivate()));
        store.setPublicKey(publicKeyToBase64(keyPair.getPublic()));
        return store;
    }

    public static void main(String[] args) {
        try {
            // 1. Генерация ключей
            KeyPair keyPair = generateKeyPair();
            System.out.println("Ключи сгенерированы успешно!");

            // 2. Сохранение ключей в Base64
            String privateKeyBase64 = privateKeyToBase64(keyPair.getPrivate());
            String publicKeyBase64 = publicKeyToBase64(keyPair.getPublic());
            System.out.println("Приватный ключ (Base64): " + privateKeyBase64);
            System.out.println("Публичный ключ (Base64): " + publicKeyBase64);

            // 3. Данные для подписи
            String message = "Hello, Ed25519!";
            byte[] data = message.getBytes();

            // 4. Подпись данных
            byte[] signature = sign(keyPair.getPrivate(), data);
            String signatureBase64 = signatureToBase64(signature);
            System.out.println("Подпись (Base64): " + signatureBase64);

            // 5. Проверка подписи
            boolean isValid = verify(keyPair.getPublic(), data, signature);
            System.out.println("Подпись валидна: " + isValid);

            // 6. Загрузка ключей из Base64
            PrivateKey loadedPrivateKey = privateKeyFromBase64(privateKeyBase64);
            PublicKey loadedPublicKey = publicKeyFromBase64(publicKeyBase64);

            // 7. Проверка загруженными ключами
            boolean isValidWithLoaded = verify(loadedPublicKey, data, signature);
            System.out.println("Проверка загруженными ключами: " + isValidWithLoaded);

            // 8. Удобный метод подписи -> Base64
            String signatureBase64Direct = signToBase64(keyPair.getPrivate(), data);
            boolean isValidDirect = verifyFromBase64(keyPair.getPublic(), data, signatureBase64Direct);
            System.out.println("Проверка через удобные методы: " + isValidDirect);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class KeyPairStore {
        private String privateKey;
        private String publicKey;

        public String getPrivateKey() { return privateKey; }
        public void setPrivateKey(String privateKey) { this.privateKey = privateKey; }
        public String getPublicKey() { return publicKey; }
        public void setPublicKey(String publicKey) { this.publicKey = publicKey; }
    }
}