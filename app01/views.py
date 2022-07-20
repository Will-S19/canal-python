# Create your views here.
import pickle

from rest_framework.generics import ListAPIView
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from app01.models import Book
from app01.serializer import BookSerializer
from mq.workers import redis





class BookView(ListAPIView):
    permission_classes = (AllowAny,)
    serializer_class = BookSerializer
    queryset = Book.objects.all()

    def list(self, request, *args, **kwargs):
        books = redis.get("book")
        if books:
            queryset = Book.objects.filter(id__in=books)
        else:
            queryset = self.filter_queryset(self.get_queryset())

        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)

        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)

    def get(self, request, *args, **kwargs):
        return self.list(request, *args, **kwargs)


