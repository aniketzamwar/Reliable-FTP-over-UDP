/*
 * commonFunctions.cpp
 * Common Functions and Variables used in client and server
 *
 * Code written by Aniket Zamwar
 * zamwar@usc.edu
 * designed by Aniket and Abhishek
 */

#include "common.h"
#include <openssl/md5.h>


//Global variables used in client and server
FILE *log_fp;
pthread_mutex_t logger_file_lock = PTHREAD_MUTEX_INITIALIZER;

extern sockaddr_in client_addr, server_addr;
extern socklen_t server_udp_sock_len, client_udp_sock_len;

/*
 * Error Handling Function
 */
void error(const char *msg){

    perror(msg);
    exit(1);
}

void sigUSR1Handler(int signum){
	pthread_exit(NULL);
}

/*
 * initiates udp connection.
 * Future Scope function. Need to modify.
 */
int setup_udp_socket(int portno,struct hostent *server)
{
	/*printf("\nOpening socket at %d",portno);
	int sockfd;
	struct sockaddr_in *temp_addr;
	sockfd = socket(AF_INET, SOCK_DGRAM, 0);
	if (sockfd < 0){
		error("ERROR opening socket");
		return ERROR;
	}
	int optval = 1;
	setsockopt(sockfd,SOL_SOCKET,SO_REUSEADDR,&optval, sizeof optval);

	bzero((char *) &temp_addr, sizeof(temp_addr));
	temp_addr.sin_family = AF_INET;
	if(server == NULL){
		temp_addr.sin_addr.s_addr = INADDR_ANY;
		bcopy(&client_addr,&temp_addr,sizeof(temp_addr));
		client_udp_sock_len = sizeof(client_addr);
	}
	else{
		bcopy((char *)server->h_addr,
		      (char *)&temp_addr.sin_addr.s_addr,
		      server->h_length);
		bcopy(&server_addr,&temp_addr,sizeof(temp_addr));
		server_udp_sock_len = sizeof(server_addr);
	}
	temp_addr.sin_port = htons(portno);
	if (bind(sockfd, (struct sockaddr *) &temp_addr,sizeof(temp_addr)) < 0){
		error("ERROR on binding");
		return ERROR;
	}
	printf("\nSockfd %d",sockfd);
	return sockfd;*/
}

/*
 * convert message structure to char stream to send over network.
 */
char* message_to_stream(struct message message_struct)
{
	char *message_stream = NULL;
	char *message_stream_ptr = NULL;
	//int len_of_message = 5 + strlen(message_struct.data) + 1;
	int len_of_message = message_struct.length + 1;
	//printf("\n<message_to_stream> length of message is %d and length of data is %d",len_of_message,strlen(message_struct.data));
	//printf("\n<message_to_stream> length of message is %d and length of file data is %d",len_of_message - 1,message_struct.length - 5);

	message_stream = (char *)malloc(len_of_message);
	message_stream_ptr = message_stream;
	memset(message_stream,'\0',len_of_message);

	memcpy(message_stream,&(message_struct.type),1);
	message_stream+=1;

	if(message_struct.type == ACK){
		memcpy(message_stream,message_struct.data,strlen(message_struct.data));
	}
	else{

		//printf("\nMessage Type Copied");
		uint32_t network_byte_order;
		network_byte_order = htonl(message_struct.seq_num);

		memcpy(message_stream,&network_byte_order,4);
		message_stream+=4;
		//printf("\nMessage seq no Copied");

		//memcpy(message_stream,message_struct.data,strlen(message_struct.data));
		memcpy(message_stream,message_struct.data,message_struct.length - 5);
		//printf("\nMessage data Copied");

	}
	return message_stream_ptr;
}

/*
 * Convert stream message received from network to structure.
 */
struct message* stream_to_message(char *message_stream,int len_of_message)
{
	struct message *message_struct;
	message_struct = (struct message *)malloc(sizeof(struct message));
	message_struct -> data = NULL;

	memcpy(&message_struct->type,message_stream,1);
	message_stream+=1;

	message_struct->length = len_of_message;
	// if it is DATA type message then only we have data to be written to file
	if(message_struct->type == DATA)
	{
		uint32_t network_byte_order;
		memcpy(&network_byte_order,message_stream,4);
		message_struct->seq_num = ntohl(network_byte_order);

		message_stream+=4;

		len_of_message = len_of_message - 5;
		message_struct->data = (char *)malloc(len_of_message + 1);
		memset(message_struct->data,'\0',len_of_message + 1);
		memcpy(message_struct->data,message_stream,len_of_message);
	}
	else if(message_struct->type == ACK || message_struct->type == NOTIFY){
		len_of_message = len_of_message - 1;
		message_struct->data = (char *)malloc(len_of_message + 1);
		memset(message_struct->data,'\0',len_of_message + 1);
		memcpy(message_struct->data,message_stream,len_of_message);
		//printf("\nReceived ACK is %s",message_struct->data);
	}
	//printf("\n<stream_to_message> Message Received is: Type: %d, Seq No. %d",message_struct->type,message_struct->seq_num);

	return message_struct;
}

/*
 * Function to Log sent and Received Messages,
 * And to log the md5 of small packets of file received and sent.
 */
void logger_system(struct message *message_struct, int sent_or_received){

	/*if(log_fp == NULL)
		return;

	pthread_mutex_lock(&logger_file_lock);

	if(sent_or_received == RESENT){
			time_t current_time = time(NULL);
			if(message_struct->type == ACK){
				fprintf (log_fp, "[%d] RESENT ACKW SEQ-No: %s\n",current_time,message_struct->data);
			}
			else if(message_struct->type == DATA){
				unsigned char md5_buf[16];
				MD5((const unsigned char *)message_struct->data,message_struct->length-5,md5_buf);
				fprintf (log_fp, "[%d] RESENT DATA SEQ-No: %d, Len: %d",current_time,message_struct->seq_num,message_struct->length);
				for(int i=0;i<16;i++){
					fprintf (log_fp, "%02x",md5_buf[i]);
				}
				fprintf (log_fp,"\n");
			}
		}
	else if(sent_or_received == SENT){
		time_t current_time = time(NULL);
		if(message_struct->type == ACK){
			fprintf (log_fp, "[%d] SENT ACKW SEQ-No: %s\n",current_time,message_struct->data);
		}
		else if(message_struct->type == DATA){
			unsigned char md5_buf[16];
			MD5((const unsigned char *)message_struct->data,message_struct->length-5,md5_buf);
			fprintf (log_fp, "[%d] SENT DATA SEQ-No: %d, Len: %d",current_time,message_struct->seq_num,message_struct->length);
			for(int i=0;i<16;i++){
				fprintf (log_fp, "%02x",md5_buf[i]);
			}
			fprintf (log_fp,"\n");
		}
	}
	else if(sent_or_received == RECV){
		time_t current_time = time(NULL);
		if(message_struct->type == ACK){
			fprintf (log_fp, "[%d] RECV ACKW SEQ-No: %s\n",current_time,message_struct->data);
		}
		else if(message_struct->type == DATA){
			unsigned char md5_buf[16];
			MD5((const unsigned char *)message_struct->data,message_struct->length - 5,md5_buf);
			fprintf (log_fp, "[%d] RECV DATA SEQ-No: %d, Len: %d",current_time,message_struct->seq_num,message_struct -> length);
			for(int i=0;i<16;i++){
				fprintf (log_fp, "%02x",md5_buf[i]);
			}
			fprintf (log_fp,"\n");
		}
	}
	fflush(log_fp);
	pthread_mutex_unlock(&logger_file_lock);*/
}
