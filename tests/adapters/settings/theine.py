SECRET_KEY = "django_tests_secret_key"
CACHES = {
    "default": {
        "BACKEND": "theine.adapters.django.Cache",
        "TIMEOUT": 60,
        "OPTIONS": {"MAX_ENTRIES": 1000, "POLICY": "tlfu"},
    },
}

INSTALLED_APPS = [
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
]

USE_TZ = False
