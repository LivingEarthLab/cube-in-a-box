"""
JupyterHub custom authenticator for restricting user signups.

This module contains a custom authenticator and signup handler that overrides
the default NativeAuthenticator behavior. It validates new signups against
an allowed list of users defined in the JupyterHub configuration, preventing
unauthorized account creation while still allowing open signups for approved users.
"""
from nativeauthenticator import NativeAuthenticator
from nativeauthenticator.handlers import SignUpHandler, LocalBase
from tornado import web


class CustomSignUpHandler(SignUpHandler):
    """Custom signup handler that validates username against allowed users on submission."""

    # Remove the get() override - show normal signup form to everyone

    async def post(self):
        """Override POST to check if username is in allowed list before processing."""

        # 404 if users aren't allowed to sign up
        if not self.authenticator.enable_signup:
            raise web.HTTPError(404)

        # Get the username from the form
        username = self.get_body_argument("username", strip=False)

        # Check if username is in allowed users
        allowed_users = self.authenticator.allowed_users

        if allowed_users and username not in allowed_users:
            # User not in allowed list, show signup form with error message
            html = await self.render_template(
                "signup.html",
                ask_email=self.authenticator.ask_email_on_signup,
                two_factor_auth=self.authenticator.allow_2fa,
                recaptcha_key=self.authenticator.recaptcha_key,
                tos=self.authenticator.tos,
                result_message=f"The username '{username}' is not authorized for signup. Please contact the administrator to be added to the authorized users list.",
                alert="alert-danger",
            )
            self.finish(html)
        else:
            # Enforce that a password is provided (default=None avoids Tornado 400 when field is absent)
            # NativeAuthenticator uses 'signup_password', not 'password', to avoid browser autofill conflicts
            password = self.get_body_argument("signup_password", default=None, strip=False)
            if not password:
                html = await self.render_template(
                    "signup.html",
                    ask_email=self.authenticator.ask_email_on_signup,
                    two_factor_auth=self.authenticator.allow_2fa,
                    recaptcha_key=self.authenticator.recaptcha_key,
                    tos=self.authenticator.tos,
                    result_message="A password is required. Please enter a password before signing up.",
                    alert="alert-danger",
                )
                self.finish(html)
                return

            # User is in allowed list and provided a password, proceed with normal signup
            await super().post()


class CustomNativeAuthenticator(NativeAuthenticator):
    """Custom authenticator that uses our custom signup handler."""

    def get_handlers(self, app):
        """Override to use custom signup handler."""
        # Get default handlers from NativeAuthenticator
        handlers = super().get_handlers(app)

        # Replace the SignUpHandler with our custom one
        custom_handlers = []
        for pattern, handler in handlers:
            if handler == SignUpHandler:
                custom_handlers.append((pattern, CustomSignUpHandler))
            else:
                custom_handlers.append((pattern, handler))

        return custom_handlers
