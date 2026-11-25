package com.example.payment.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.security.Principal;
import java.util.UUID;
import com.example.payment.api.dto.ApiError;
import com.example.payment.api.dto.ChangePaymentMethodRequest;
import com.example.payment.api.dto.PaymentResponseDTO;
import com.example.payment.api.error.PaymentRetryNotAllowedException;

@RestController
@RequestMapping("/api/payments")
@RequiredArgsConstructor
@Validated
@SecurityRequirement(name = "basicAuth")
@Tag(name = "Payment API", description = "Get payment status and results and retry failed payments")
@ApiResponses({
    @ApiResponse(responseCode = "401", description = "Authentication is required",
        content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = ApiError.class))),
    @ApiResponse(responseCode = "403", description = "Access denied or CSRF token missing or invalid",
        content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = ApiError.class))),
    @ApiResponse(responseCode = "400", description = "Invalid ID",
        content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = ApiError.class))),
    @ApiResponse(responseCode = "404", description = "Payment not found",
        content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = ApiError.class))),
    @ApiResponse(responseCode = "503", description = "Database is not available",
        content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = ApiError.class))),
    @ApiResponse(responseCode = "500", description = "Internal server error",
        content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = ApiError.class)))
})
class PaymentController {

    private static final String INVALID_PAYMENT_ID_MESSAGE = "Payment ID must be a whole number greater than 0";

    private final PaymentQueryService paymentQueryService;
    private final PaymentActionService paymentActionService;

    @GetMapping("/{id}")
    @Operation(summary = "Get payment status and result")
    @ApiResponse(responseCode = "200", description = "Payment found",
        content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = PaymentResponseDTO.class)))
    public ResponseEntity<PaymentResponseDTO> getPayment(
        @PathVariable("id") @Positive(message = INVALID_PAYMENT_ID_MESSAGE) Long id
    ) {
        return ResponseEntity.ok()
            .cacheControl(CacheControl.noStore())
            .body(paymentQueryService.findById(id));
    }

    @GetMapping("/by-event-id/{eventId}")
    @Operation(
        summary = "Get payment by event ID",
        description = "404 means the payment is not in the database or is not accessible to this user. "
            + "Its message may still be waiting to be processed."
    )
    @ApiResponse(responseCode = "200", description = "Payment found",
        content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = PaymentResponseDTO.class)))
    public ResponseEntity<PaymentResponseDTO> getPaymentByEventId(
        @PathVariable("eventId") UUID eventId
    ) {
        return ResponseEntity.ok()
            .cacheControl(CacheControl.noStore())
            .body(paymentQueryService.findByEventId(eventId));
    }

    @PostMapping("/{id}/payment-method")
    @Operation(summary = "Confirm the same payment with another card")
    @ApiResponse(responseCode = "202", description = "Card change queued")
    @ApiResponse(responseCode = "409", description = "Payment or card has changed, or a new card is not needed")
    public ResponseEntity<Void> changePaymentMethod(
        @PathVariable("id") @Positive(message = INVALID_PAYMENT_ID_MESSAGE) Long id,
        @Valid @RequestBody ChangePaymentMethodRequest request, Principal owner
    ) {
        if (!paymentActionService.changePaymentMethod(id, owner.getName(), request)) {
            return ResponseEntity.status(409).build();
        }
        return ResponseEntity.accepted().build();
    }

    @PostMapping("/{id}/retry")
    @Operation(
        summary = "Retry a failed payment",
        description = "The owner or an administrator can return a FAILED payment to NEW with a fresh attempt budget. "
            + "Requires a CSRF token from GET /api/csrf and the same session cookie. Other payment states return 409."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "204", description = "Payment queued for retry", content = @Content),
        @ApiResponse(responseCode = "409", description = "Payment is not FAILED",
            content = @Content(mediaType = "application/json",
                schema = @Schema(implementation = ApiError.class)))
    })
    public ResponseEntity<Void> retryPayment(
        @PathVariable("id") @Positive(message = INVALID_PAYMENT_ID_MESSAGE) Long id
    ) {
        if (!paymentActionService.retryFailedPayment(id)) {
            throw new PaymentRetryNotAllowedException(id);
        }
        return ResponseEntity.noContent()
            .cacheControl(CacheControl.noStore())
            .build();
    }
}
