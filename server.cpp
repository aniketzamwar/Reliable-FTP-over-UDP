/*
 * Client.cpp
 * File Sender
 *
 * Code written by Abhishek Prabhudesai
 * prabhude@usc.edu
 * designed by Aniket and Abhishek
 */

#include "server.h"
//#include "serverThreads.h"

/*extern void sigUSR1Handler(int);
extern void error(const char *);
extern void senderThread();
extern void receiverThread();
extern void fileWriterThread();*/

extern struct message* stream_to_message(char *,int);
extern char* message_to_stream(struct message);
extern void sigUSR1Handler(int);
void sigUSR2Handler(int);
extern void error(const char *);
extern void logger_system(struct message *, int);

extern FILE *log_fp;
extern pthread_mutex_t logger_file_lock;

time_t start_time,end_time;

void handle_tcp_control_connection(){

	server_tcp_sock_len = sizeof(int);
	//Setup TCP control connection
	server_tcp_socket = socket(AF_INET, SOCK_STREAM, 0);
	if (server_tcp_socket < 0)
		error("ERROR opening socket");
	bzero((char *) &server_tcp_addr, sizeof(server_tcp_addr));
	//server_tcp_port = atoi((char *) port);
	server_tcp_addr.sin_family = AF_INET;
	server_tcp_addr.sin_addr.s_addr = INADDR_ANY;
	server_tcp_addr.sin_port = htons(server_tcp_port);
	if((setsockopt(server_tcp_socket,SOL_SOCKET,SO_REUSEADDR,&server_tcp_sock_opt,server_tcp_sock_len)) == -1){
		printf("Error in setting socket opt");
		exit(0);
	}
	if (bind(server_tcp_socket, (struct sockaddr *) &server_tcp_addr,sizeof(server_tcp_addr)) < 0)
		error("ERROR on binding");
	listen(server_tcp_socket,5);
	client_tcp_len = sizeof(client_tcp_addr);
	server_tcp_control_socket = accept(server_tcp_socket,(struct sockaddr *) &client_tcp_addr, &client_tcp_len);
	close(server_tcp_socket);
	return;
}

void create_and_send_ack_message(int seq_num){

	int num_of_bytes;
	char * sender_buffer;
	//printf("\nCreated ACK msg with seq num = %d\n",seq_num);
	//Create the message
	struct message msg_to_send;
	msg_to_send.type = ACK;
	msg_to_send.seq_num = seq_num;
	msg_to_send.data = NULL;
	msg_to_send.length = 5;
	sender_buffer = message_to_stream(msg_to_send);
	int n = write(server_tcp_control_socket,sender_buffer,msg_to_send.length);
	//printf("\nACK sent\n");
	free(sender_buffer);
}

void sendFinalAcksAndExit(){
	
	if(!sender_thread_queue.empty()){
		char * ack_array = NULL;
		uint64_t seq_num_for_ack;
		ack_array = (char *)malloc(200000);
		memset(ack_array,'\0',200000);
		seq_num_for_ack = sender_thread_queue.front();
		sender_thread_queue.pop();
		sprintf(ack_array,"lu",seq_num_for_ack);
		while(! sender_thread_queue.empty()){
			seq_num_for_ack = sender_thread_queue.front();
			sender_thread_queue.pop();
			sprintf(ack_array,"%s,%lu",ack_array,seq_num_for_ack);
		}	
		//create_and_send_ack_message(seq_num_for_ack);
		char * sender_buffer;
		//printf("\n[LAST] Created ACK msg with seq num = %d\n",seq_num_for_ack);
		//Create the message
		struct message msg_to_send;
		msg_to_send.type = ACK;
		msg_to_send.seq_num = 0;
		msg_to_send.data = ack_array;
		msg_to_send.length = 1 + strlen(ack_array);
		sender_buffer = message_to_stream(msg_to_send);
		//logger_system(&msg_to_send,SENT);
		//int n = write(server_tcp_control_socket,sender_buffer,msg_to_send.length);
		int i = 0;
		while(i < 5){
			int n  = sendto(server_udp_socket,sender_buffer,msg_to_send.length,0,(struct sockaddr *) &client_addr,client_len);
			i++;
		}
		printf("\nLast ACK sent\n");
		if(sender_buffer != NULL)
			free(sender_buffer);
	}	
	//printf("\nSender exit\n");
	//raise(SIGUSR1);
	pthread_exit(NULL);
}

void senderThread(){

	/*
	 * 	1.	create a ACK message to send to the client
	 * 	2.	Use the client_addr struct and send the message to the client
	 */

	//printf("\n<-- Server: Sender Thread -->\n");

	char * ack_array = NULL;
	server_actUSR1.sa_handler = sigUSR1Handler;
	sigaction(SIGUSR1, &server_actUSR1, NULL);
	pthread_sigmask(SIG_BLOCK, &server_signalSetUSR1, NULL);

	struct timeval tvtime;

	uint64_t seq_num_for_ack;
	int num_of_acks = 0, i = 0;
	//handle_tcp_control_connection();

	while(1){

		tvtime.tv_sec = 0;
		tvtime.tv_usec = 10000;

		select(0,NULL,NULL,NULL,&tvtime);
		if(time_to_exit == 1){
			pthread_exit(NULL);
		}
		//wait for signal from receiver thread
		pthread_mutex_lock(&sender_thread_queue_Lock);
		if(sender_thread_queue.empty()){
			//pthread_cond_wait(&sender_thread_queue_CV,&sender_thread_queue_Lock);
			//printf("\nSENDER: Receiver signal from receiver thread\n");
			pthread_mutex_unlock(&sender_thread_queue_Lock);
			continue;
		}
		else{
			ack_array = (char *)malloc(200000);
			memset(ack_array,'\0',200000);
			seq_num_for_ack = sender_thread_queue.front();
			sender_thread_queue.pop();
			sprintf(ack_array,"%lu",seq_num_for_ack);
			while(! sender_thread_queue.empty()){
				seq_num_for_ack = sender_thread_queue.front();
				sender_thread_queue.pop();
				sprintf(ack_array,"%s,%lu",ack_array,seq_num_for_ack);
			}
			//create_and_send_ack_message(seq_num_for_ack);
			char * sender_buffer;
			//printf("\nCreated ACK msg with seq num = %d\n",seq_num_for_ack);
			//Create the message
			struct message msg_to_send;
			msg_to_send.type = ACK;
			msg_to_send.seq_num = 0;
			msg_to_send.data = ack_array;
			msg_to_send.length = 1 + strlen(ack_array);
			sender_buffer = message_to_stream(msg_to_send);
			//logger_system(&msg_to_send,SENT);
			//int n = write(server_tcp_control_socket,sender_buffer,msg_to_send.length);
			int n  = sendto(server_udp_socket,sender_buffer,msg_to_send.length,0,(struct sockaddr *) &client_addr,client_len);
			//printf("\nACK sent\n");
			if(sender_buffer != NULL)
				free(sender_buffer);
		}
		pthread_mutex_unlock(&sender_thread_queue_Lock);
	}
	pthread_exit(NULL);
}

void fileWriterThread(){

	server_actUSR1.sa_handler = sigUSR1Handler;
	sigaction(SIGUSR1, &server_actUSR1, NULL);
	pthread_sigmask(SIG_BLOCK, &server_signalSetUSR1, NULL);
	recvd_fptr = fopen("received_file","a+");
	uint64_t next_seq_no_to_write = 1;
	uint64_t total_bytes_received = 0;
	printf("\nReceiving . . . ");
	while(1){

		struct message * msg = NULL;
		pthread_mutex_lock(&file_writer_thread_Lock);
		file_writer_cache_iter = file_writer_cache.find(next_seq_no_to_write);
		if(file_writer_cache_iter == file_writer_cache.end()){
			pthread_cond_wait(&file_writer_thread_CV,&file_writer_thread_Lock);
		}
		if((file_writer_cache_iter = file_writer_cache.find(next_seq_no_to_write)) !=  file_writer_cache.end()){
			message *msg = file_writer_cache_iter->second;
			file_writer_cache.erase(file_writer_cache_iter->first);
			pthread_mutex_unlock(&file_writer_thread_Lock);
			int bytes_written = fwrite(msg->data,1,(msg->length - 5),recvd_fptr);
			fflush(recvd_fptr);
			if(msg->data != NULL){
				free(msg->data);
			}
			if(msg != NULL){
				free(msg);
				msg = NULL;
			}
			next_seq_no_to_write++;
			total_bytes_received = total_bytes_received + bytes_written;
			//if(total_bytes_received >= 209715200){
			if(total_bytes_received >= 1048576000){
			//if(total_bytes_received >= 524288000){

				end_time = time(NULL);
				uint32_t time_diff = end_time - start_time;
				fprintf(log_fp,"%d\n",time_diff);
				fclose(log_fp);
				fclose(recvd_fptr);
				printf("\nFile Received!\n");
				pthread_mutex_unlock(&file_writer_thread_Lock);
				
				break;
			}
			//cout<<"\nTotal bytes written = "<<total_bytes_received<<endl;
		}
		else{
			pthread_mutex_unlock(&file_writer_thread_Lock);
		}
	}
	pthread_exit(NULL);
}

void receiverThread(){

	server_actUSR1.sa_handler = sigUSR1Handler;
	sigaction(SIGUSR1, &server_actUSR1, NULL);
	pthread_sigmask(SIG_BLOCK, &server_signalSetUSR1, NULL);
	int recvd_seq_num;
	//printf("\n<-- Server: Receiver Thread -->\n");

	start_time = time(NULL);

	while(1){
		recvd_seq_num = 0;
		message * recvd_msg = NULL;
		//message * parsedMsg = NULL;
		//wait for signal
		pthread_mutex_lock(&receiver_thread_queue_Lock);
		if(receiver_thread_queue.empty()){
			pthread_cond_wait(&receiver_thread_queue_CV,&receiver_thread_queue_Lock);
			if(time_to_exit == 1){
				pthread_mutex_unlock(&receiver_thread_queue_Lock);
				pthread_exit(NULL);
			}
			//printf("\n[Receiver] out of wait\n");
		}
		recvd_msg = receiver_thread_queue.front();
		receiver_thread_queue.pop();
		pthread_mutex_unlock(&receiver_thread_queue_Lock);

		switch(recvd_msg->type){
			case DATA:{

				recvd_seq_num = recvd_msg->seq_num;
				acked_msgs_map_iter = acked_msgs_map.find(recvd_seq_num);
				if(acked_msgs_map_iter == acked_msgs_map.end()){

					//packet is received for the first time, Need to write it
					pthread_mutex_lock(&file_writer_thread_Lock);
					//file_writer_queue.push(recvd_msg);
					file_writer_cache.insert(CACHEMAP::value_type(recvd_seq_num, recvd_msg));
					pthread_cond_signal(&file_writer_thread_CV);
					pthread_mutex_unlock(&file_writer_thread_Lock);
					/*pthread_t new_file_writer;
					pthread_create(&new_file_writer,NULL,(void* (*)(void*))fileWriterThread,recvd_msg);*/
				}
				break;
			}
			case ACK:{
				//not used for now
				break;
			}
			case NOTIFY:{
				//signal all the threads to exit

				fclose(recvd_fptr);
				pthread_kill(sender_thread,SIGUSR1);
				pthread_kill(file_writer_thread,SIGUSR1);

				time_to_exit = 1;
				close(server_udp_socket);
				close(server_tcp_control_socket);

				pthread_exit(NULL);
				break;
			}
			default:{
				printf("\nInvalid message type\n");
				break;
			}
		}
	}
	pthread_exit(NULL);
}

void initialize_queues(){

	while(!sender_thread_queue.empty()){
		sender_thread_queue.pop();
	}

	while(!receiver_thread_queue.empty()){
		receiver_thread_queue.pop();
	}

	while(!file_writer_queue.empty()){
		file_writer_queue.pop();
	}

	for(file_writer_cache_iter = file_writer_cache.begin(); file_writer_cache_iter != file_writer_cache.end(); file_writer_cache_iter++){
		file_writer_cache.erase(file_writer_cache_iter->first);
	}
}

int main(int argc, char *argv[]){

	//char * server_buffer = NULL;
	//const struct sched_param *param;
	//pthread_setschedparam(sender_thread, SCHED_RR,param);
	//pthread_setschedparam(receiver_thread, SCHED_RR,param);
	//pthread_setschedparam(timer_thread, SCHED_RR,param);
	
	log_fp = fopen("time_elapsed_file_receiver.log","w+");
	if(log_fp == NULL)
		printf("Could not open log file\n");

	int num_of_bytes = 0;
	uint64_t recv_sock_buffer_size = 1000000000;	//1024x1024x1024
	//uint64_t recv_sock_buffer_size = 64000;	//1024x1024x1024
	uint64_t total_bytes_recvd = 0;
	server_udp_socket = socket(AF_INET, SOCK_DGRAM, 0);
	server_udp_sock_len = sizeof(int);

	sigemptyset(&server_signalSetUSR1);
	server_actUSR1.sa_handler = sigUSR1Handler;
	sigaction(SIGUSR1, &server_actUSR1, NULL);
	pthread_sigmask(SIG_UNBLOCK, &server_signalSetUSR1, NULL);

	if (server_udp_socket < 0)
	   error("ERROR opening socket");
	bzero((char *) &server_addr, sizeof(server_addr));
	server_port = atoi(argv[1]);
	//server_udp_socket = setup_udp_socket(server_port , NULL);

	server_addr.sin_family = AF_INET;
	server_addr.sin_addr.s_addr = INADDR_ANY;
	server_addr.sin_port = htons(server_port);

	//pthread_create(&tcp_control_thread,NULL,(void* (*)(void*))tcpControlThread,argv[2]);

	if((setsockopt(server_udp_socket,SOL_SOCKET,SO_REUSEADDR,&server_sock_opt,server_udp_sock_len)) == -1){
		printf("Error in setting socket opt");
	}
	int status = setsockopt(server_udp_socket,SOL_SOCKET,SO_RCVBUF,&recv_sock_buffer_size, sizeof(uint64_t));
	if(status < 0)
		error("\nError setting socket buffer size");
	/*int sizebuf = 0;
	socklen_t sizelen = sizeof(sizebuf);
	uint64_t size = getsockopt(server_udp_socket, SOL_SOCKET,SO_RCVBUF, &sizebuf, &sizelen);
	cout<<"\nServer socket buff = "<<size<<"\n";*/

	if (bind(server_udp_socket, (struct sockaddr *) &server_addr,sizeof(server_addr)) < 0)
	    error("ERROR on binding");
	client_len = sizeof(client_addr);

	//initialize_window();
	initialize_queues();

	//server_tcp_port = atoi(argv[2]);
	//create sender and receiver threads
	(void) pthread_create(&sender_thread,NULL,(void* (*)(void*))senderThread,NULL);
	(void) pthread_create(&receiver_thread,NULL,(void* (*)(void*))receiverThread,NULL);
	//(void) pthread_create(&timer_thread,NULL,(void* (*)(void*))timerThread,NULL);
	(void) pthread_create(&file_writer_thread,NULL,(void* (*)(void*))fileWriterThread,NULL);

	//printf("\nServer Started at port %d IP Address %s\n",server_port,inet_ntoa(server_addr.sin_addr));
	bzero((char *) &client_addr, sizeof(client_addr));
	recvd_fptr = fopen("received_file","w");
	while(1){
		char * server_buffer = NULL;
		struct message* msg = NULL;

		server_buffer = (char *)malloc(500000);
		memset(server_buffer, '\0', 500000);

		num_of_bytes = recvfrom(server_udp_socket,server_buffer,500000,0,(struct sockaddr *)&client_addr,&client_len);
		if(num_of_bytes < 0 && time_to_exit == 1)
			break;
		else{
			//printf("\nNum of byte received = %d\n",num_of_bytes);
			msg = stream_to_message(server_buffer, num_of_bytes);
			//logger_system(msg,RECV);
			//printf("\nReceived %d bytes from : %s from port %d\n", msg->length,inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));
		}
		if(msg->type == NOTIFY){

			time_to_exit = 1;
			pthread_mutex_lock(&sender_thread_queue_Lock);
			pthread_cond_signal(&sender_thread_queue_CV);
			pthread_mutex_unlock(&sender_thread_queue_Lock);
			pthread_mutex_lock(&receiver_thread_queue_Lock);
			pthread_cond_signal(&receiver_thread_queue_CV);
			pthread_mutex_unlock(&receiver_thread_queue_Lock);
			break;
		}
		int seq_no;
		seq_no = msg->seq_num;

		pthread_mutex_lock(&sender_thread_queue_Lock);
		sender_thread_queue.push(seq_no);
		pthread_cond_signal(&sender_thread_queue_CV);
		pthread_mutex_unlock(&sender_thread_queue_Lock);

		pthread_mutex_lock(&receiver_thread_queue_Lock);
		receiver_thread_queue.push(msg);
		pthread_cond_signal(&receiver_thread_queue_CV);
		pthread_mutex_unlock(&receiver_thread_queue_Lock);
		free(server_buffer);
	}
	//fclose(recvd_fptr);
	pthread_join(sender_thread,NULL);
	pthread_join(receiver_thread,NULL);
	pthread_join(file_writer_thread,NULL);
	close(server_udp_socket);
	return 0;
}
