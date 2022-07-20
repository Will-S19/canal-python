from rest_framework import serializers

from app01.models import Book


class BookSerializer(serializers.ModelSerializer):
    class Meta:
        model = Book
        fields = ("__all__")
