from django.db import models


class ModelWithDateTime(models.Model):
    create_at = models.DateTimeField(auto_now_add=True)
    update_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


# Create your models here.
class Author(ModelWithDateTime):
    name = models.CharField(max_length=30, blank=True)
    phone = models.CharField(max_length=30)
    email = models.EmailField(blank=True, null=True)


class Book(ModelWithDateTime):
    name = models.CharField(max_length=30, blank=True)
    publisher = models.CharField(max_length=30, blank=True, null=True)
    price = models.IntegerField(blank=True, null=True)
