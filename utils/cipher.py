from cryptography.fernet import Fernet


class CipherUtils(object):
    __key = b'NZqFKZPTlQuEEVzNhVXGtpWdeiVxc8y9Vg1fpkwJBUg='
    __cipher_suite = Fernet(__key)

    @staticmethod
    def encrypt(plain_message: str) -> str:
        if not isinstance(plain_message, str):
            raise TypeError('Message to encrypt must be a string.')

        return CipherUtils.__cipher_suite.encrypt(plain_message)

    @staticmethod
    def decrypt(encrypted_message: str) -> str:
        if not isinstance(encrypted_message, str):
            raise TypeError('Message to decrypt must be a string.')

        return CipherUtils.__cipher_suite.decrypt(encrypted_message)
