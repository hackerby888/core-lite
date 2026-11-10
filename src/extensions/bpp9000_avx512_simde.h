#pragma once

// Lets an AVX2-only x86 build compile the AVX-512 bpp9000 engine: SIMDe maps each AVX-512 intrinsic to AVX2 code.
// __AVX512F__ stays undefined, so everything else in the translation unit keeps its AVX2 path.
#if defined(LITE_BPP9000_AVX512_VIA_SIMDE) && !defined(__AVX512F__)

#include <lib/platform_common/qintrin.h>

#define SIMDE_ENABLE_NATIVE_ALIASES
#include <lib/platform_common/simde/x86/avx512.h>
#include <lib/platform_common/simde/x86/gfni.h>

// Upstream SIMDe defines these as `(k1) & call(...)` with no outer parentheses, so `(cast)MACRO << n` misparses.
// The vendored copy adds them; without them sizeof binds to (k1) alone and this stops being a constant expression.
#define LITE_SIMDE_MASK_CMP_IS_PARENTHESIZED(macro, maskType, zero) \
    static_assert(sizeof macro((maskType)0, zero, zero, 0) == sizeof(int), #macro " lost its outer parentheses")
LITE_SIMDE_MASK_CMP_IS_PARENTHESIZED(simde_mm512_mask_cmp_epi16_mask, simde__mmask32, simde_mm512_setzero_si512());
LITE_SIMDE_MASK_CMP_IS_PARENTHESIZED(simde_mm512_mask_cmp_epu16_mask, simde__mmask32, simde_mm512_setzero_si512());
LITE_SIMDE_MASK_CMP_IS_PARENTHESIZED(simde_mm256_mask_cmp_epu32_mask, simde__mmask8, simde_mm256_setzero_si256());
LITE_SIMDE_MASK_CMP_IS_PARENTHESIZED(simde_mm512_mask_cmp_epu32_mask, simde__mmask16, simde_mm512_setzero_si512());
LITE_SIMDE_MASK_CMP_IS_PARENTHESIZED(simde_mm512_mask_cmp_epu64_mask, simde__mmask8, simde_mm512_setzero_si512());
#undef LITE_SIMDE_MASK_CMP_IS_PARENTHESIZED

// 512-bit values travel through memory when a call is not inlined.
#define LITE_SIMD_INLINE static inline __attribute__((always_inline))

// A 512-bit value as its two AVX2 halves.
struct LiteYmmPair
{
    __m256i lo;
    __m256i hi;
};

LITE_SIMD_INLINE LiteYmmPair liteSplit512(__m512i value)
{
    LiteYmmPair halves;
    __builtin_memcpy(&halves, &value, sizeof(halves));
    return halves;
}

LITE_SIMD_INLINE __m512i liteJoin512(__m256i lo, __m256i hi)
{
    const LiteYmmPair halves = { lo, hi };
    __m512i value;
    __builtin_memcpy(&value, &halves, sizeof(value));
    return value;
}

// Bit i of mask -> byte i all-ones.
LITE_SIMD_INLINE __m256i liteMaskToBytes(unsigned int mask)
{
    const __m256i byteOfLane = _mm256_setr_epi8(0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3);
    const __m256i bitOfByte = _mm256_set1_epi64x((long long)0x8040201008040201ULL);
    const __m256i spread = _mm256_shuffle_epi8(_mm256_set1_epi32((int)mask), byteOfLane);
    return _mm256_cmpeq_epi8(_mm256_and_si256(spread, bitOfByte), bitOfByte);
}

// Bit i of mask -> dword i all-ones (8 lanes).
LITE_SIMD_INLINE __m256i liteMaskToDwords(unsigned int mask)
{
    const __m256i bitOfLane = _mm256_setr_epi32(1, 2, 4, 8, 16, 32, 64, 128);
    return _mm256_cmpeq_epi32(_mm256_and_si256(_mm256_set1_epi32((int)mask), bitOfLane), bitOfLane);
}

// Bit i of mask -> qword i all-ones (4 lanes).
LITE_SIMD_INLINE __m256i liteMaskToQwords(unsigned int mask)
{
    const __m256i bitOfLane = _mm256_setr_epi64x(1, 2, 4, 8);
    return _mm256_cmpeq_epi64(_mm256_and_si256(_mm256_set1_epi64x((long long)mask), bitOfLane), bitOfLane);
}

LITE_SIMD_INLINE unsigned int liteDwordsToMask(__m256i lanes)
{
    return (unsigned int)_mm256_movemask_ps(_mm256_castsi256_ps(lanes));
}

LITE_SIMD_INLINE unsigned long long liteBytesToMask512(__m256i lo, __m256i hi)
{
    return (unsigned long long)(unsigned int)_mm256_movemask_epi8(lo) | ((unsigned long long)(unsigned int)_mm256_movemask_epi8(hi) << 32);
}

LITE_SIMD_INLINE __m512i liteSet1Epi8(char value)
{
    const __m256i half = _mm256_set1_epi8(value);
    return liteJoin512(half, half);
}
#undef _mm512_set1_epi8
#define _mm512_set1_epi8(value) liteSet1Epi8((char)(value))

// --- byte masks ---

LITE_SIMD_INLINE __m256i liteMaskMovEpi8_256(__m256i src, unsigned int mask, __m256i a)
{
    return _mm256_blendv_epi8(src, a, liteMaskToBytes(mask));
}

LITE_SIMD_INLINE __m512i liteMaskMovEpi8_512(__m512i src, unsigned long long mask, __m512i a)
{
    const LiteYmmPair s = liteSplit512(src);
    const LiteYmmPair v = liteSplit512(a);
    return liteJoin512(liteMaskMovEpi8_256(s.lo, (unsigned int)mask, v.lo), liteMaskMovEpi8_256(s.hi, (unsigned int)(mask >> 32), v.hi));
}

LITE_SIMD_INLINE unsigned long long liteCmpeqEpi8Mask512(__m512i a, __m512i b)
{
    const LiteYmmPair x = liteSplit512(a);
    const LiteYmmPair y = liteSplit512(b);
    return liteBytesToMask512(_mm256_cmpeq_epi8(x.lo, y.lo), _mm256_cmpeq_epi8(x.hi, y.hi));
}

LITE_SIMD_INLINE unsigned long long liteTestEpi8Mask512(__m512i a, __m512i b)
{
    const LiteYmmPair x = liteSplit512(a);
    const LiteYmmPair y = liteSplit512(b);
    const __m256i zero = _mm256_setzero_si256();
    return ~liteBytesToMask512(_mm256_cmpeq_epi8(_mm256_and_si256(x.lo, y.lo), zero), _mm256_cmpeq_epi8(_mm256_and_si256(x.hi, y.hi), zero));
}

#undef _mm256_mask_mov_epi8
#define _mm256_mask_mov_epi8(src, mask, a) liteMaskMovEpi8_256((src), (unsigned int)(mask), (a))
#undef _mm512_mask_mov_epi8
#define _mm512_mask_mov_epi8(src, mask, a) liteMaskMovEpi8_512((src), (unsigned long long)(mask), (a))
#undef _mm256_mask_cmpeq_epi8_mask
#define _mm256_mask_cmpeq_epi8_mask(mask, a, b) ((__mmask32)((unsigned int)(mask) & (unsigned int)_mm256_movemask_epi8(_mm256_cmpeq_epi8((a), (b)))))
#undef _mm512_mask_cmpeq_epi8_mask
#define _mm512_mask_cmpeq_epi8_mask(mask, a, b) ((__mmask64)((unsigned long long)(mask) & liteCmpeqEpi8Mask512((a), (b))))
#undef _mm512_cmpneq_epi8_mask
#define _mm512_cmpneq_epi8_mask(a, b) ((__mmask64)~liteCmpeqEpi8Mask512((a), (b)))
#undef _mm512_test_epi8_mask
#define _mm512_test_epi8_mask(a, b) ((__mmask64)liteTestEpi8Mask512((a), (b)))
#undef _mm512_mask2_permutex2var_epi8
#define _mm512_mask2_permutex2var_epi8(a, idx, mask, b) liteMaskMovEpi8_512((idx), (unsigned long long)(mask), _mm512_permutex2var_epi8((a), (idx), (b)))

// --- dword and qword masks (16 and 8 lanes) ---

LITE_SIMD_INLINE __m512i liteMaskMovEpi32(__m512i src, unsigned int mask, __m512i a)
{
    const LiteYmmPair s = liteSplit512(src);
    const LiteYmmPair v = liteSplit512(a);
    return liteJoin512(_mm256_blendv_epi8(s.lo, v.lo, liteMaskToDwords(mask & 0xFF)), _mm256_blendv_epi8(s.hi, v.hi, liteMaskToDwords((mask >> 8) & 0xFF)));
}

LITE_SIMD_INLINE __m512i liteMaskMovEpi64(__m512i src, unsigned int mask, __m512i a)
{
    const LiteYmmPair s = liteSplit512(src);
    const LiteYmmPair v = liteSplit512(a);
    return liteJoin512(_mm256_blendv_epi8(s.lo, v.lo, liteMaskToQwords(mask & 0x0F)), _mm256_blendv_epi8(s.hi, v.hi, liteMaskToQwords((mask >> 4) & 0x0F)));
}

LITE_SIMD_INLINE unsigned int liteCmpeqEpi32Mask(__m512i a, __m512i b)
{
    const LiteYmmPair x = liteSplit512(a);
    const LiteYmmPair y = liteSplit512(b);
    return liteDwordsToMask(_mm256_cmpeq_epi32(x.lo, y.lo)) | (liteDwordsToMask(_mm256_cmpeq_epi32(x.hi, y.hi)) << 8);
}

// Signed a > b per dword.
LITE_SIMD_INLINE unsigned int liteCmpgtEpi32Mask(__m512i a, __m512i b)
{
    const LiteYmmPair x = liteSplit512(a);
    const LiteYmmPair y = liteSplit512(b);
    return liteDwordsToMask(_mm256_cmpgt_epi32(x.lo, y.lo)) | (liteDwordsToMask(_mm256_cmpgt_epi32(x.hi, y.hi)) << 8);
}

// Unsigned a > b per dword: flipping the sign bit turns it into the signed compare.
LITE_SIMD_INLINE unsigned int liteCmpgtEpu32Mask(__m512i a, __m512i b)
{
    const __m512i signBit = _mm512_set1_epi32((int)0x80000000U);
    return liteCmpgtEpi32Mask(_mm512_xor_si512(a, signBit), _mm512_xor_si512(b, signBit));
}

#undef _mm512_mask_mov_epi32
#define _mm512_mask_mov_epi32(src, mask, a) liteMaskMovEpi32((src), (unsigned int)(mask), (a))
#undef _mm512_maskz_mov_epi32
#define _mm512_maskz_mov_epi32(mask, a) liteMaskMovEpi32(_mm512_setzero_si512(), (unsigned int)(mask), (a))
#undef _mm512_mask_add_epi32
#define _mm512_mask_add_epi32(src, mask, a, b) liteMaskMovEpi32((src), (unsigned int)(mask), _mm512_add_epi32((a), (b)))
#undef _mm512_cmpneq_epi32_mask
#define _mm512_cmpneq_epi32_mask(a, b) ((__mmask16)(~liteCmpeqEpi32Mask((a), (b)) & 0xFFFF))
#undef _mm512_mask_cmpneq_epi32_mask
#define _mm512_mask_cmpneq_epi32_mask(mask, a, b) ((__mmask16)((unsigned int)(mask) & ~liteCmpeqEpi32Mask((a), (b)) & 0xFFFF))
#undef _mm512_mask_cmpeq_epi32_mask
#define _mm512_mask_cmpeq_epi32_mask(mask, a, b) ((__mmask16)((unsigned int)(mask) & liteCmpeqEpi32Mask((a), (b))))
#undef _mm512_mask_cmpge_epi32_mask
#define _mm512_mask_cmpge_epi32_mask(mask, a, b) ((__mmask16)((unsigned int)(mask) & ~liteCmpgtEpi32Mask((b), (a)) & 0xFFFF))
#undef _mm512_mask_cmpgt_epu32_mask
#define _mm512_mask_cmpgt_epu32_mask(mask, a, b) ((__mmask16)((unsigned int)(mask) & liteCmpgtEpu32Mask((a), (b))))
#undef _mm512_mask_or_epi64
#define _mm512_mask_or_epi64(src, mask, a, b) liteMaskMovEpi64((src), (unsigned int)(mask), _mm512_or_si512((a), (b)))
#undef _mm512_mask_srli_epi64
#define _mm512_mask_srli_epi64(src, mask, a, count) liteMaskMovEpi64((src), (unsigned int)(mask), _mm512_srli_epi64((a), (count)))

// --- gf2p8affine: the engine only passes the identity matrix or its mirror, which is an 8x8 bit transpose per qword ---

// out.byte[i].bit[k] = in.byte[k].bit[i] in every qword.
LITE_SIMD_INLINE __m256i liteBitTranspose8x8(__m256i x)
{
    __m256i t = _mm256_and_si256(_mm256_xor_si256(x, _mm256_srli_epi64(x, 7)), _mm256_set1_epi64x(0x00AA00AA00AA00AALL));
    x = _mm256_xor_si256(x, _mm256_xor_si256(t, _mm256_slli_epi64(t, 7)));
    t = _mm256_and_si256(_mm256_xor_si256(x, _mm256_srli_epi64(x, 14)), _mm256_set1_epi64x(0x0000CCCC0000CCCCLL));
    x = _mm256_xor_si256(x, _mm256_xor_si256(t, _mm256_slli_epi64(t, 14)));
    t = _mm256_and_si256(_mm256_xor_si256(x, _mm256_srli_epi64(x, 28)), _mm256_set1_epi64x(0x00000000F0F0F0F0LL));
    x = _mm256_xor_si256(x, _mm256_xor_si256(t, _mm256_slli_epi64(t, 28)));
    return x;
}

// out.byte[i].bit[k] = in.byte[7 - k].bit[7 - i] in every qword.
LITE_SIMD_INLINE __m256i liteBitAntiTranspose8x8(__m256i x)
{
    __m256i t = _mm256_xor_si256(x, _mm256_slli_epi64(x, 36));
    x = _mm256_xor_si256(x, _mm256_and_si256(_mm256_set1_epi64x((long long)0xF0F0F0F00F0F0F0FULL), _mm256_xor_si256(t, _mm256_srli_epi64(x, 36))));
    t = _mm256_and_si256(_mm256_set1_epi64x((long long)0xCCCC0000CCCC0000ULL), _mm256_xor_si256(x, _mm256_slli_epi64(x, 18)));
    x = _mm256_xor_si256(x, _mm256_xor_si256(t, _mm256_srli_epi64(t, 18)));
    t = _mm256_and_si256(_mm256_set1_epi64x((long long)0xAA00AA00AA00AA00ULL), _mm256_xor_si256(x, _mm256_slli_epi64(x, 9)));
    x = _mm256_xor_si256(x, _mm256_xor_si256(t, _mm256_srli_epi64(t, 9)));
    return x;
}

LITE_SIMD_INLINE __m256i liteGf2p8Affine256(__m256i x, __m256i matrix, int constant)
{
    const __m256i identity = _mm256_set1_epi64x((long long)0x8040201008040201ULL);
    const __m256i mirrored = _mm256_set1_epi64x((long long)0x0102040810204080ULL);
    __m256i result;
    if (_mm256_movemask_epi8(_mm256_cmpeq_epi8(x, mirrored)) == -1)
    {
        result = liteBitAntiTranspose8x8(matrix);
    }
    else if (_mm256_movemask_epi8(_mm256_cmpeq_epi8(x, identity)) == -1)
    {
        const __m256i reverseBytes = _mm256_setr_epi8(7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8);
        result = liteBitTranspose8x8(_mm256_shuffle_epi8(matrix, reverseBytes));
    }
    else
    {
        result = simde_mm256_gf2p8affine_epi64_epi8(x, matrix, 0);
    }
    return _mm256_xor_si256(result, _mm256_set1_epi8((char)constant));
}

LITE_SIMD_INLINE __m512i liteGf2p8Affine512(__m512i x, __m512i matrix, int constant)
{
    const LiteYmmPair v = liteSplit512(x);
    const LiteYmmPair m = liteSplit512(matrix);
    return liteJoin512(liteGf2p8Affine256(v.lo, m.lo, constant), liteGf2p8Affine256(v.hi, m.hi, constant));
}

#undef _mm256_gf2p8affine_epi64_epi8
#define _mm256_gf2p8affine_epi64_epi8(x, matrix, constant) liteGf2p8Affine256((x), (matrix), (constant))
#undef _mm512_gf2p8affine_epi64_epi8
#define _mm512_gf2p8affine_epi64_epi8(x, matrix, constant) liteGf2p8Affine512((x), (matrix), (constant))

// --- multishift: byte i of each qword = bits [c, c + 8) of that qword rotated, c = control byte & 63 ---

LITE_SIMD_INLINE __m256i liteMultishift256(__m256i control, __m256i data)
{
    const __m256i lowBytes = _mm256_set1_epi16(0x00FF);
    const __m256i seven = _mm256_set1_epi8(7);
    const __m256i qwordBase = _mm256_setr_epi8(0, 0, 0, 0, 0, 0, 0, 0, 8, 8, 8, 8, 8, 8, 8, 8, 0, 0, 0, 0, 0, 0, 0, 0, 8, 8, 8, 8, 8, 8, 8, 8);
    // 1 << (8 - bitShift) for bitShift 1..7; a zero shift is patched below.
    const __m256i scaleTable = _mm256_setr_epi8(0, (char)0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0, 0, 0, 0, 0, 0, 0, 0,
                                                0, (char)0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0, 0, 0, 0, 0, 0, 0, 0);

    const __m256i byteShift = _mm256_and_si256(_mm256_srli_epi16(control, 3), seven);
    const __m256i bitShift = _mm256_and_si256(control, seven);
    const __m256i first = _mm256_shuffle_epi8(data, _mm256_or_si256(byteShift, qwordBase));
    const __m256i nextIndex = _mm256_and_si256(_mm256_add_epi8(byteShift, _mm256_set1_epi8(1)), seven);
    const __m256i second = _mm256_shuffle_epi8(data, _mm256_or_si256(nextIndex, qwordBase));
    const __m256i scale = _mm256_shuffle_epi8(scaleTable, bitShift);

    // 16-bit window [first, second] >> bitShift, done as a high multiply; even and odd byte positions separately.
    const __m256i evenWindow = _mm256_or_si256(_mm256_and_si256(first, lowBytes), _mm256_slli_epi16(_mm256_and_si256(second, lowBytes), 8));
    const __m256i evenScale = _mm256_slli_epi16(_mm256_and_si256(scale, lowBytes), 8);
    const __m256i evenResult = _mm256_and_si256(_mm256_mulhi_epu16(evenWindow, evenScale), lowBytes);
    const __m256i oddWindow = _mm256_or_si256(_mm256_srli_epi16(first, 8), _mm256_andnot_si256(lowBytes, second));
    const __m256i oddScale = _mm256_andnot_si256(lowBytes, scale);
    const __m256i oddResult = _mm256_slli_epi16(_mm256_mulhi_epu16(oddWindow, oddScale), 8);

    const __m256i shifted = _mm256_or_si256(evenResult, oddResult);
    return _mm256_blendv_epi8(shifted, first, _mm256_cmpeq_epi8(bitShift, _mm256_setzero_si256()));
}

LITE_SIMD_INLINE __m512i liteMultishift512(__m512i control, __m512i data)
{
    const LiteYmmPair c = liteSplit512(control);
    const LiteYmmPair d = liteSplit512(data);
    return liteJoin512(liteMultishift256(c.lo, d.lo), liteMultishift256(c.hi, d.hi));
}

#undef _mm512_multishift_epi64_epi8
#define _mm512_multishift_epi64_epi8(control, data) liteMultishift512((control), (data))

// --- intrinsics SIMDe 0.8.4 does not provide ---

#undef _mm512_broadcast_i64x2
#define _mm512_broadcast_i64x2(a) _mm512_broadcast_i32x4(a)

LITE_SIMD_INLINE __m512i liteMaskI32GatherEpi32(__m512i src, unsigned int mask, __m512i index, const void* base, int scale)
{
    int indices[16];
    int lanes[16];
    _mm512_storeu_si512((void*)indices, index);
    _mm512_storeu_si512((void*)lanes, src);
    for (int i = 0; i < 16; ++i)
    {
        if ((mask >> i) & 1)
        {
            __builtin_memcpy(&lanes[i], (const char*)base + (long long)indices[i] * scale, sizeof(int));
        }
    }
    return _mm512_loadu_si512((const void*)lanes);
}
#undef _mm512_mask_i32gather_epi32
#define _mm512_mask_i32gather_epi32(src, mask, index, base, scale) liteMaskI32GatherEpi32((src), (unsigned int)(mask), (index), (base), (scale))

#endif
