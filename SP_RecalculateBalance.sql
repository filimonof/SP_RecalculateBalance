/* <============================================================================================
Name:           P_RecalculateBalance
Author:			Filimonov Vitaliy
Description:    Recalculate balances and repaid invoices
Create Date:	20.02.2019
================================================================================================> */
create procedure dbo.P_RecalculateBalance
	@LeaseID bigint
as
begin
	declare 	
		@Today date = cast(getdate() as date),
		@PaymentStatusesCompleted int = 98,
		@PaymentStatusesPending int = 99

	begin try
		begin transaction recalculateBalance
				
		if not exists(select 1 from dbo.T_LeaseAccounts where LeaseID = @LeaseID)
		begin
			insert into dbo.T_LeaseAccounts values (@LeaseID, 0, 0, 0)
		end

		if (select count(*) from dbo.T_LeaseAccounts where LeaseID = @LeaseID) > 1
		begin			
			delete from dbo.T_LeaseAccounts
			where LeaseID = @LeaseID
				and LeaseAccountID not in 
				(
					select top 1 LeaseAccountID 
					from dbo.T_LeaseAccounts 
					where LeaseID = @LeaseID 
					order by LeaseAccountID desc
				)
		end
				
		delete from dbo.T_PaymentInvoiceMapping
		where LeaseID = @LeaseID

		;with invoicesIntervals as -- строим список счетов с накоплением суммы
		(
			select 
				InvoiceID, 
				[Money],		
				sum([Money]) over (												-- нарастающий итог
					partition by LeaseID order by DueOnDate asc, InvoiceID asc
					rows between unbounded preceding and current row
				) as intervalBabla
			from dbo.T_Invoices		
			where 
				LeaseID = @LeaseID 			
				and IsActive = 1
		),
		paymentsIntervals as -- строим список платежей с накоплением суммы
		(
			select 
				PaymentID, 
				[Money],
				sum([Money]) over (													-- нарастающий итог
					partition by LeaseID order by PaymentDate asc, PaymentID asc	-- CreatedDate or PaymentDate
					rows between unbounded preceding and current row
				) as intervalBabla 						
			from dbo.T_Payments		
			where 
				LeaseID = @LeaseID 			
				and IsActive = 1
				and [Status] in (@PaymentStatusesCompleted, @PaymentStatusesPending)
		),
		unionIntervals as	-- объединяем списки счетов и платежей
		(
			select intervalBabla               
			from invoicesIntervals
			union
			select intervalBabla               
			from paymentsIntervals
		),
		mappingInvoicesAndPayments as -- таблица маппинга счетов и платежей
		(
			select *,
				row_number() over(order by intervalBabla asc) as RowNumber
			from unionIntervals
			outer apply 
			(
				select top(1) InvoiceID
				from invoicesIntervals
				where invoicesIntervals.intervalBabla >= unionIntervals.intervalBabla
				order by invoicesIntervals.intervalBabla        
			) as invoices
			outer apply 
			(
				select top(1) PaymentID
				from paymentsIntervals
				where paymentsIntervals.intervalBabla >= unionIntervals.intervalBabla
				order by paymentsIntervals.intervalBabla        
			) as payments	
		),
		mappingInvoicesAndPaymentsAndAmount as  -- таблица маппинга счетов и платежей и внесенной суммы
		(
			select cur.*,
				cur.intervalBabla - isnull(pre.intervalBabla, 0) as Amount
			from mappingInvoicesAndPayments cur
			left join mappingInvoicesAndPayments pre on cur.RowNumber = pre.RowNumber + 1
		)
		insert into dbo.T_PaymentInvoiceMapping (LeaseID, PaymentID, InvoiceID, Amount)
		select @LeaseID, PaymentID,	InvoiceID,	Amount
		from mappingInvoicesAndPaymentsAndAmount
				
		;with newPreparedInvoices as  -- подготавливаем данные для правки счетов
		(
			select 
				T_Invoices.InvoiceID, 
				T_Invoices.Money,	
				T_Invoices.Paid, 	
				T_Invoices.PaidDate, 
				T_Invoices.PaymentRest, 
				sum(isnull(T_PaymentInvoiceMapping.Amount, 0)) as Paymented,
				max(T_Payments.PaymentDate) as PaymentedDate
			from dbo.T_Invoices 
			left join dbo.T_PaymentInvoiceMapping on T_PaymentInvoiceMapping.InvoiceID = T_Invoices.InvoiceID and T_PaymentInvoiceMapping.PaymentID is not null
			left join dbo.T_Payments on T_Payments.PaymentID = T_PaymentInvoiceMapping.PaymentID and T_Payments.IsActive = 1
			where T_Invoices.LeaseID = @LeaseID		
			group by 
				T_Invoices.InvoiceID, 
				T_Invoices.Money, 
				T_Invoices.Paid, 
				T_Invoices.PaidDate, 
				T_Invoices.PaymentRest
		)		
		update dbo.T_Invoices	-- правим счета
		set 
			Paid = iif(prepared.Money = prepared.Paymented, 1, 0),
			PaymentRest = prepared.Money - prepared.Paymented,
			PaidDate = prepared.PaymentedDate						
		from newPreparedInvoices as prepared
		where T_Invoices.InvoiceID = prepared.InvoiceID
		
		-- обновляем баланс
		update T_LeaseAccounts
		set T_LeaseAccounts.Balance = T_LeaseAccounts.PreExistingBalance + isnull(invoices.SumInvoice, 0) - isnull(payments.SymPayments, 0), 
			T_LeaseAccounts.Debt = isnull(invoices.SumDebt, 0)
		from dbo.T_LeaseAccounts 
			left join (
				select 
					T_Invoices.LeaseID,
					sum(T_Invoices.Money) as SumInvoice,
					sum(iif(T_Invoices.ExpiryDate <= @Today and T_Invoices.Paid = 0, T_Invoices.PaymentRest, 0)) as SumDebt
				from dbo.T_Invoices
				where T_Invoices.IsActive = 1 				
					and T_Invoices.LeaseID = @LeaseID				
				group by T_Invoices.LeaseID
			) invoices on invoices.LeaseID = T_LeaseAccounts.LeaseID		
			left join (
				select 
					T_Payments.LeaseID,
					sum(T_Payments.Money) as SymPayments
				from dbo.T_Payments
				where T_Payments.isActive = 1
					and T_Payments.LeaseID = @LeaseID
					and T_Payments.Status in (@PaymentStatusesCompleted, @PaymentStatusesPending)		
				group by T_Payments.LeaseID
			) payments on payments.LeaseID = T_LeaseAccounts.LeaseID								
		where T_LeaseAccounts.LeaseID = @LeaseID

		commit transaction recalculateBalance		

    end try
    begin catch	    		
		if (@@trancount > 0) rollback transaction recalculateBalance
    end catch
end
go

