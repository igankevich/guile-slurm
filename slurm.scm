; SPDX-License-Identifier: GPL-3.0-or-later

(define-module (slurm)
  #:use-module (ice-9 format)
  #:use-module (ice-9 popen)
  #:use-module (ice-9 receive)
  #:use-module (ice-9 regex)
  #:use-module (ice-9 textual-ports)
  #:use-module (oop goops)
  #:use-module (srfi srfi-1))

(define-class <dependency> ()
  (type #:init-keyword #:type #:accessor dependency-type #:init-value #f)
  (ids #:init-keyword #:ids #:accessor dependency-ids #:init-value '())
  (jobs #:init-keyword #:jobs #:accessor dependency-jobs #:init-value '()))

(define (after . rest)
  (make <dependency>
    #:type "after"
    #:jobs rest))

(define-public (dependency->string dependency)
  (let ((ids (map (lambda (id) (format #f "~a" id)) (dependency-ids dependency)))
        (more-ids (fold
                    (lambda (job prev)
                      (if (job-id job)
                        (cons (format #f "~a" (job-id job)) prev)
                        prev))
                    '()
                    (dependency-jobs dependency))))
    (define all-ids (append ids more-ids))
    (if (null? all-ids)
      ""
      (string-join (cons (dependency-type dependency) all-ids) ":"))))

;(define-method (write (o <dependency>) out)
;  (write (dependency->string o) out))

(define-class <job> ()
  (id #:init-keyword #:id #:accessor job-id #:init-value #f)
  (name #:init-keyword #:name #:accessor job-name #:init-value #f)
  (output-filename #:init-keyword #:output-filename #:accessor job-output-filename
                   #:init-value #f)
  (error-filename #:init-keyword #:error-filename #:accessor job-error-filename
                  #:init-value #f)
  (user #:init-keyword #:user #:accessor job-user #:init-value #f)
  (group #:init-keyword #:group #:accessor job-group #:init-value #f)
  (script #:init-keyword #:script #:accessor job-script #:init-value #f)
  (arguments #:init-keyword #:arguments #:accessor job-arguments #:init-value '())
  (more-arguments #:init-keyword #:more-arguments #:accessor job-more-arguments
                  #:init-value '())
  (environment #:init-keyword #:environment #:accessor job-environment #:init-value '())
  (partitions #:init-keyword #:partitions #:accessor job-partitions #:init-value '())
  (min-node-count #:init-keyword #:min-node-count #:accessor job-min-node-count
                  #:init-value #f)
  (max-node-count #:init-keyword #:max-node-count #:accessor job-max-node-count
                  #:init-value #f)
  (working-directory #:init-keyword #:working-directory #:accessor job-working-directory
                     #:init-value #f)
  (comment #:init-keyword #:comment #:accessor job-comment #:init-value #f)
  (wait? #:init-keyword #:wait? #:accessor job-wait? #:init-value #f)
  (oversubscribe? #:init-keyword #:oversubscribe? #:accessor job-oversubscribe?
                  #:init-value #f)
  (overcommit? #:init-keyword #:overcommit? #:accessor job-overcommit?
               #:init-value #f)
  (contiguous? #:init-keyword #:contiguous? #:accessor job-contiguous? #:init-value #f)
  (spread? #:init-keyword #:spread? #:accessor job-spread? #:init-value #f)
  (test? #:init-keyword #:test? #:accessor job-test? #:init-value #f)
  (dependencies #:init-keyword #:dependencies #:accessor job-dependencies #:init-value '())
  (time #:init-keyword #:time #:accessor job-time #:init-value #f))

(define* (make-job name script #:optional (arguments '()))
  (make <job>
    #:name name
    #:script script
    #:arguments arguments))

(define (shell-escape str)
  (string-append
    "'"
    (string-fold-right
      (lambda (char prev)
        (if (char=? char #\')
          (string-append "\\'" prev)
          (string-append (string char) prev)))
      ""
      str)
    "'"))

(define (with-temporary-file proc)
  (define port (mkstemp! (string-copy "/dev/shm/tmp-XXXXXX")))
  (define filename (port-filename port))
  (catch #t
    (lambda () (proc port filename))
    (lambda (key . parameters)
      (close-port port)
      (delete-file filename)
      (throw key parameters))))

(define-public (job-submit job)
  (with-temporary-file
    (lambda (script-port script-filename)
      (define command
        `("sbatch"
          "--parsable"
          ,@(if (null? (job-partitions job)) '()
              (list (string-append "--partition=" (string-join (job-partitions job) ","))))
          ,@(let ((min-nodes (job-min-node-count job))
                  (max-nodes (job-max-node-count job)))
              (if (or min-nodes max-nodes)
                `(,(format #f "--nodes=~a-~a"
                           (if min-nodes min-nodes max-nodes)
                           (if max-nodes max-nodes min-nodes)))
                '()))
          ,@(if (job-name job) (list (string-append "--job-name=" (job-name job))) '())
          ,@(if (job-id job) (list (string-append "--jobid=" (job-id job))) '())
          ,@(if (job-user job) (list (string-append "--uid=" (job-user job))) '())
          ,@(if (job-group job) (list (string-append "--gid=" (job-group job))) '())
          ,@(if (job-working-directory job)
              (list (string-append "--chdir=" (job-working-directory job))) '())
          ,@(if (job-output-filename job)
              (list (string-append "--output=" (job-output-filename job))) '())
          ,@(if (job-error-filename job)
              (list (string-append "--error=" (job-error-filename job))) '())
          ,@(if (job-wait? job) '("--wait") '())
          ,@(if (job-contiguous? job) '("--contiguous") '())
          ,@(if (job-spread? job) '("--spread-job") '())
          ,@(if (job-oversubscribe? job) '("--oversubscribe") '())
          ,@(if (job-overcommit? job) '("--overcommit") '())
          ,@(if (job-test? job) '("--test-only") '())
          ,@(if (null? (job-dependencies job)) '()
              (list (string-append
                      "--dependency="
                      (string-join (map dependency->string (job-dependencies job)) ","))))
          ,@(if (job-time job) `(,(format #f "--time=~a" (job-time job))) '())
          ,@(if (job-comment job)
              (list (string-append "--comment=" (job-comment job))) '())
          ,@(if (job-environment job)
              (let* ((variables (job-environment job))
                     (new-variables
                       (cond
                         ((null? variables) '())
                         ((equal? (car variables) #t) (cons "ALL" (cdr variables)))
                         ((equal? (car variables) #f) (cons "NONE" (cdr variables)))
                         (else variables))))
                (if (null? new-variables)
                  '()
                  (list (string-append "--export=" (string-join new-variables ",")))))
              '())
          ,@(job-more-arguments job)
          ,(let ()
             (call-with-output-file script-filename
               (lambda (port) (display (job-script job) port)))
             (chmod script-filename #o0755)
             script-filename)
          ,@(job-arguments job)))
      (with-temporary-file
        (lambda (error-output-port error-output-filename)
          (receive (status output)
            (with-error-to-port error-output-port
              (lambda ()
                (define port (open-input-pipe (string-join (map shell-escape command) " ")))
                (define output (string-trim-both (get-string-all port)))
                (values (close-pipe port) output)))
            (close-port error-output-port)
            (let ((id (if (= (status:exit-val status) 0)
                        (string->number output)
                        (call-with-input-file error-output-filename get-string-all))))
              (set! (job-id job) id)
              (values status id))))))))

;;(define-public (job-wait id)
;;  (define ids (if (list? id) id (list id)))
;;  (define dummy-job
;;    (make <job>
;;      #:name (string-append "wait-for--" (string-join (map number->string ids) "-"))
;;      #:script "#!/bin/sh\n# waiting for jobs...\n"
;;      #:dependencies (list (make <dependency> #:type "afterany" #:ids ids))
;;      #:wait? #t))
;;  (job-submit dummy-job))

(define-class <job-status> ()
  (id #:init-keyword #:id #:accessor job-status-id #:init-value #f)
  (name #:init-keyword #:name #:accessor job-status-name #:init-value #f)
  (state #:init-keyword #:state #:accessor job-status-state #:init-value #f)
  (exit-code #:init-keyword #:exit-code #:accessor job-status-exit-code #:init-value #f)
  (step-exit-code #:init-keyword #:step-exit-code #:accessor job-status-step-exit-code
                  #:init-value #f)
  (output-filename #:init-keyword #:output-filename #:accessor job-status-output-filename
                   #:init-value #f)
  (error-filename #:init-keyword #:error-filename #:accessor job-status-error-filename
                  #:init-value #f)
  (comment #:init-keyword #:comment #:accessor job-status-comment #:init-value #f))

(define (job-status-success? status)
  (and
    (= (job-status-exit-code status) 0)
    (= (job-status-step-exit-code status) 0)
    (string=? (job-status-state status) "COMPLETED")))

(define (job-status-cancel status)
  (if (list? status)
    (map job-status-cancel status)
    (system* "scancel" (number->string (job-status-id status)))))

(define (alist->job-status alist)
  (define exit-code (assoc-ref alist "ExitCode"))
  (make <job-status>
    #:id (assoc-ref alist "JobId")
    #:name (assoc-ref alist "JobName")
    #:state (assoc-ref alist "JobState")
    #:exit-code (car exit-code)
    #:step-exit-code (cdr exit-code)
    #:error-filename (assoc-ref alist "StdErr")
    #:output-filename (assoc-ref alist "StdOut")
    #:comment (assoc-ref alist "Comment")))

(define %slurm-fields '(
  "JobId" "JobName" "UserId" "GroupId" "MCS_label" "Priority" "Nice" "Account" "QOS"
  "JobState" "Reason" "Dependency" "Requeue" "Restarts" "BatchFlag" "Reboot" "ExitCode"
  "RunTime" "TimeLimit" "TimeMin" "SubmitTime" "EligibleTime" "StartTime" "EndTime"
  "Deadline" "PreemptTime" "SuspendTime" "SecsPreSuspend" "LastSchedEval" "Partition"
  "AllocNode:Sid" "ReqNodeList" "ExcNodeList" "NodeList" "BatchHost" "NumNodes"
  "NumCPUs" "NumTasks" "CPUs/Task" "ReqB:S:C:T" "TRES" "Socks/Node" "NtasksPerN:B:S:C"
  "CoreSpec" "MinCPUsNode" "MinMemoryNode" "MinTmpDiskNode" "Features" "DelayBoot"
  "Gres" "Reservation" "OverSubscribe" "Contiguous" "Licenses" "Network" "Command"
  "WorkDir" "StdErr" "StdIn" "StdOut" "Power" "Comment"))
(define rx-user-id (make-regexp "^(.*)\\(([0-9]+)\\)$"))
(define rx-group-id rx-user-id)
(define rx-exit-code (make-regexp "^([-0-9]+):([-0-9]+)$"))
(define rx-field (make-regexp "([a-zA-Z0-9_/:]+)="))

(define (slurm-parse-field key value)
  (cons key
        (cond
          ((string= key "JobId") (string->number value))
          ((string= key "UserId")
           (let ((match (regexp-exec rx-user-id value)))
             (if match
               (cons (match:substring match 1) (string->number (match:substring match 2)))
               value)))
          ((string= key "GroupId")
           (let ((match (regexp-exec rx-group-id value)))
             (if match
               (cons (match:substring match 1) (string->number (match:substring match 2)))
               value)))
          ((string= key "ExitCode")
           (let ((match (regexp-exec rx-exit-code value)))
             (if match
               (cons (string->number (match:substring match 1))
                     (string->number (match:substring match 2)))
               value)))
          ((string= key "NodeList") (if (string= value "(null)") "" value))
          (else value))))

(define (parse-slurm str)
  (define (find-next-field start)
    (define match (regexp-exec rx-field str start))
    (if match
        (let ((key (match:substring match 1)))
          (if (member key %slurm-fields) match (find-next-field (match:end match 0))))
        #f))
  (define match (find-next-field 0))
  (define end (if match (match:end match 0) 0))
  (define first-key (if match (match:substring match 1) ""))
  (define first-value (if match (string-trim-both (substring str end)) ""))
  (define (parse-rest obj objects start)
    (define match (find-next-field start))
    ;;(format #t "~a ~a ~a\n" (match:substring match 1) (match:start match 1)
    ;;        (match:end match 1))
    (if match
        (let* ((new-start (match:start match 0))
               (new-end (match:end match 0))
               (key (match:substring match 1))
               (value (substring str new-end)))
          (define updated-object
            (cons (cons (caar obj) (string-trim-both (substring str start new-start)))
                  (cdr obj)))
          (if (string= key first-key)
              (parse-rest (cons (cons key value) '()) (cons updated-object objects) new-end)
              (parse-rest (cons* (cons key value) updated-object) objects new-end)))
        (cons obj objects)))
  (if match (parse-rest (cons (cons first-key first-value) '()) '() end) '()))

(define* (job-status #:optional (pred (const #t)))
  (define cmd (string-append "scontrol show job --oneliner --all"))
  (define port (open-input-pipe cmd))
  (define output (get-string-all port))
  (close-pipe port)
  (map
    (lambda (alist)
      (alist->job-status
        (map (lambda (pair) (slurm-parse-field (car pair) (cdr pair))) alist)))
    (filter pred (parse-slurm output))))

(define* (wait-for-jobs pred #:key (poll-interval 60) (num-jobs #f) (callback #f))
  (define results '())
  (for-each
    (lambda (signal)
      (sigaction signal
                 (lambda (sig)
                   (for-each
                     (lambda (pair)
                       (define status (cdr pair))
                       (job-status-cancel status))
                     results))))
    (list SIGTERM SIGINT))
  (while #t
    (let ((new-results (job-status pred))
          (time-string (strftime "%Y-%m-%dT%H:%M:%S%z" (localtime (current-time)))))
      (for-each
        (lambda (status)
          (set! results (assoc-set! results (job-status-id status) status)))
        new-results)
      (if (null? results)
        (format (current-error-port) "no jobs found\n")
        (for-each
          (lambda (pair)
            (define status (cdr pair))
            (format (current-error-port) "~a job-status ~a ~a ~a ~a\n"
                    time-string
                    (job-status-id status)
                    (job-status-name status)
                    (job-status-state status)
                    (job-status-exit-code status)))
          results))
      (if callback (callback (map cdr results)))
      (force-output (current-error-port))
      (if (fold
            (lambda (pair prev)
              (define status (cdr pair))
              (and (member (job-status-state status)
                           '("COMPLETED" "CANCELLED" "FAILED" "TIMEOUT"))
                   prev))
            (or
              (and (not num-jobs) (not (null? results))) ;; at least one job
              (and num-jobs (>= (length results) num-jobs))) ;; num-jobs or more
            results)
        (break (map cdr results)))
      (sleep poll-interval))))

(define (find-all-jobs old-jobs)
  (define new-jobs
    (append-map dependency-jobs
                (append-map job-dependencies old-jobs)))
  (define filtered-jobs
    (filter
      (lambda (job) (not (memq job old-jobs)))
      new-jobs))
  (if (null? filtered-jobs)
    old-jobs
    (find-all-jobs (append filtered-jobs old-jobs))))

(define* (submit-jobs jobs #:key (wait? #t))
  (define all-jobs (find-all-jobs jobs))
  (define job-ids
    (fold
      (lambda (job ids)
        (receive (status id)
          (job-submit job)
          (if (= (status:exit-val status) 0)
            (cons id ids)
            (begin
              (format (current-error-port)
                      "failed to submit job \"~a\" (exit code = ~a): ~a\n"
                      (job-name job) status (string-trim-right id))
              ids))))
      '()
      all-jobs))
  (if (or (null? job-ids) (not wait?))
    '()
    (wait-for-jobs
      (lambda (alist)
        (member (string->number (assoc-ref alist "JobId")) job-ids)))))

;; export all symbols
(module-map
  (lambda (sym var)
    (module-export! (current-module) (list sym)))
  (current-module))
