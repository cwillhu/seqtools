"""
Django settings for seqstats project.

For more information on this file, see
https://docs.djangoproject.com/en/1.6/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/1.6/ref/settings/
"""

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
import os
BASE_DIR = os.path.dirname(os.path.dirname(__file__))

if BASE_DIR == '/Users/williams03/d/django/seqstats':
    RUN_HOME = '/Users/williams03/d/django/seqstats/seqstats/test/primary_data'
    HIST_DIR = '/Volumes/informatics_external/seq/seqstats_bkup'
    LOGFILE = '/Volumes/informatics_external/seq/seqstats_log/log.txt'
else:
    RUN_HOME = '/n/illumina01/primary_data'
    HIST_DIR = '/n/informatics_external/seq/seqstats_bkup'
    LOGFILE = '/n/informatics_external/seq/seqstats_log/log.txt'


TEMPLATE_DIRS = ( BASE_DIR + "/seqstats/templates", )

MACHINE_NAME = { 'SN343'  : 'HiSeq 2000', #map machine id to human-friendly name
                 'D00365' : 'HiSeq 2500',
                 'NS500422' : 'NextSeq' }

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/1.6/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'lih@uc)1z2g22wesd*n(pyrbhl*tvug5qx_qb9vx)@29xf_(m4'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

TEMPLATE_DEBUG = True

ALLOWED_HOSTS = []


# Application definition

INSTALLED_APPS = (
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'seqstats',
    'south',
)

MIDDLEWARE_CLASSES = (
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
)

ROOT_URLCONF = 'seqstats.urls'

WSGI_APPLICATION = 'seqstats.wsgi.application'


# Database
# https://docs.djangoproject.com/en/1.6/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': 'seqstats',
        'USER': 'cwill',
        'PASSWORD': '98smms76',
        'HOST': 'db-internal',
        'PORT': '3306',
    }
}

# Internationalization
# https://docs.djangoproject.com/en/1.6/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'EST'

USE_I18N = True

USE_L10N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.6/howto/static-files/

STATIC_ROOT = ''
STATIC_URL = '/static/'
if BASE_DIR == '/Users/williams03/d/django/seqstats':
    STATICFILES_DIRS = ( os.path.join(BASE_DIR, "static"), )
else: 
    STATICFILES_DIRS = ( os.path.join(BASE_DIR, "static/seqstats"), )
