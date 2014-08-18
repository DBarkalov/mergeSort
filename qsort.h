#pragma once


#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/thread.hpp>

// ������� ����������
class QSort
{
public:
	QSort(boost::asio::io_service& ios, int num_threads):io_service(ios), number_of_threads(num_threads)
	{	
	}
	~QSort()
	{	
	}

  // ������� ���������� �������������
  template< class _Type>
  void StartParallelQSort(_Type *s_arr, long left,  long right)
  {	  
	 io_service.reset();

     io_service.post(boost::bind(&QSort::ParallelQSort<_Type>, this, s_arr, left, right));	

	 // ���� ������������ ��������
	 if(number_of_threads < 1) number_of_threads = 2;

	 // ������ ������� �������
	 boost::thread_group tg;
	 for(int i=0;i<=number_of_threads;i++)
	 {
	 	tg.create_thread(boost::bind(&boost::asio::io_service::run, &io_service));
	 }
	
     tg.join_all();
  }


  // ���������������� ���������� - ��������� ��� ��������� ������������������
  template< class _Type >
  void SerialQSort(_Type *s_arr,  long left, long right)
  {	
	if (left >= right)
	{
		return;
	}
	 
	// ��� ��������� �������� ���������� ��������
	if ( right-left <= 20) 
	{
        InsertionSort(s_arr, left, right);
        return;
    }	

	// ������ �������� �� �������� ����������� ������ 
	long pivot = Partition(s_arr, left, right);	
	
	SerialQSort(s_arr, left, pivot - 1);
	SerialQSort(s_arr, pivot + 1, right);  	
  }

private:

// insertion sort
template< class _Type >
void InsertionSort(_Type* s_arr,  long left,  long right)
{
	   for (long i = left; i <= right; i++)  
	   {
        _Type temp = s_arr[i];
        _Type j = i;
 
        while ((j > 0) && (s_arr[j - 1] > temp)) 
		{
          s_arr[j] = s_arr[j - 1];
          j = j - 1;
        }
        s_arr[j] = temp;
      }
}


template< class _Type >
long Partition(_Type *s_arr, long i,  long j) 
{ 
	_Type pivot = s_arr[i];

	while (i < j) 
	{
		while (s_arr[j] >= pivot && i < j)
		{
			j--;
		}

		if (i < j) 
		{
			s_arr[i++] = s_arr[j];
		}

		while (s_arr[i] <= pivot && i < j) 
		{
			i++;
		}

		if (i < j) 
		{
			s_arr[j--] = s_arr[i];
		}
	}

	s_arr[i] = pivot;

	return i;
}



template< class _Type >
void ParallelQSort(_Type *s_arr,  long left,  long right)
{
	if (left >= right)
	{
		return;
	}

	 // ��� ��������� �������� ���������� ��������
     if ( right-left <= 20) 
	 {
        InsertionSort(s_arr, left, right);
        return;
     }	 

	// ������ �������� �� �������� ����������� ������ 
	long pivot = Partition(s_arr, left, right);
	
	// ��������� � ����� ������� ������ ��� ������� ������ 
	if (right-left >= 20000)
	{
		//��������� ��������� � ������� � ��������� ����������		
		io_service.post(boost::bind(&QSort::ParallelQSort<_Type>, this, s_arr, left, pivot - 1));
		io_service.post(boost::bind(&QSort::ParallelQSort<_Type>, this, s_arr, pivot + 1, right));

		return;
	}	
	// � ���� ������
	ParallelQSort<_Type>(s_arr, left, pivot - 1);
	ParallelQSort<_Type>(s_arr, pivot + 1, right);
}


private:
	boost::asio::io_service& io_service;
	int number_of_threads;  // ����� ������� ������� � �����
};

